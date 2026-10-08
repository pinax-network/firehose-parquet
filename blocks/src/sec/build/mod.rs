//! Append phase: one typed builder per table, built from its schema.
//!
//! A table is a [`Table<C>`]: the canonical identity columns, the table's own
//! columns `C` (a struct declared with [`sec_columns!`]) and the optional
//! `fork_step`/`stream_ordinal` columns. `C` is constructed from the table's
//! schema fields in order, and every column checks its schema name, so a column
//! struct that drifts from `super::schema` fails on construction (every mapper
//! test constructs every table).
//!
//! Appending one row:
//!
//! ```ignore
//! let row = self.documents.row(ctx);      // canonical + fork step
//! row.fc.append(fc);                      // the 5 filing-context columns
//! row.issuer_cik.nz(&issuer.cik);         // '' → NULL
//! row.period_of_report.opt(prep.period_of_report);
//! row.has_parse_issues.val(prep.has_parse_issues);
//! ```
//!
//! Every column of the row must be appended exactly once: a missing append
//! makes the flush fail (`RecordBatch::try_new` checks column lengths).
//!
//! Column types: [`Str`], [`Bool`], [`U32`], [`I32`], [`I64`], [`Date`], [`Ts`],
//! [`Dec`], [`Dict`], [`Bin`], [`ListStr`], [`ListDate`], [`ListI32`],
//! [`ListStruct`], and the column groups [`Fc`] (5 columns) and [`Addr`]
//! (8 columns).

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Int32Builder, Int64Builder, ListArray, ListBuilder, StringBuilder, StringDictionaryBuilder,
    StructArray, TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{
    append_fork_step, est_bin, est_bool, est_date32, est_fork_step, est_i32, est_i64, est_str,
    est_ts_ms, est_u32, estimated_dictionary_index_bytes, finish_fork_step, fork_step_builder,
    CanonicalBuilder, ForkStepBuilder, PreparedIdentity, StreamEvent,
};

use super::prepare::FilingCtx;
use super::proto::sec;
use super::schema;

pub(crate) mod beneficial;
pub(crate) mod envelope;
pub(crate) mod form13f;
pub(crate) mod form144;
pub(crate) mod formc;
pub(crate) mod formd;
pub(crate) mod hub;
pub(crate) mod issues;
pub(crate) mod ncen;
pub(crate) mod nport;
pub(crate) mod npx;
pub(crate) mod ownership;

/// What every row of one block shares: its canonical identity and stream event.
#[derive(Clone, Copy)]
pub(crate) struct AppendCtx<'a> {
    pub id: &'a PreparedIdentity,
    pub event: StreamEvent<'a>,
}

/// The schema fields still to be consumed by a column struct.
pub(crate) type FieldIter<'f> = std::slice::Iter<'f, FieldRef>;

/// A column, or a group of adjacent columns, built from its schema field(s).
pub(crate) trait Columns {
    /// Build from the next schema field(s). `name` is the struct member name:
    /// single columns check it against the field name, [`Addr`] uses it as the
    /// column prefix, and column structs and [`Fc`] ignore it.
    fn from_fields(name: &'static str, fields: &mut FieldIter<'_>) -> Self;
    /// Push the finished arrays, in schema order.
    fn finish_into(&mut self, out: &mut Vec<ArrayRef>);
    /// Logical buffer bytes (flush sizing).
    fn estimated_bytes(&self) -> usize;
}

/// The next field, which must be named `name`.
///
/// # Panics
/// If the schema has no field left or the next field has another name: the
/// column struct does not match `super::schema`.
pub(crate) fn next_field<'f>(name: &str, fields: &mut FieldIter<'f>) -> &'f Field {
    let field = fields
        .next()
        .unwrap_or_else(|| panic!("sec builder column `{name}` has no schema field left"));
    assert_eq!(
        field.name(),
        name,
        "sec builder column order differs from the schema"
    );
    field
}

/// Declare a column struct: one member per column (or column group), in schema
/// order. Generates the [`Columns`] implementation.
macro_rules! sec_columns {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $( $(#[$fmeta:meta])* $fvis:vis $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $( $(#[$fmeta])* $fvis $field: $ty, )*
        }

        impl $crate::sec::build::Columns for $name {
            fn from_fields(
                _name: &'static str,
                fields: &mut $crate::sec::build::FieldIter<'_>,
            ) -> Self {
                Self {
                    $( $field: <$ty as $crate::sec::build::Columns>::from_fields(
                        stringify!($field),
                        fields,
                    ), )*
                }
            }

            fn finish_into(&mut self, out: &mut Vec<arrow::array::ArrayRef>) {
                $( $crate::sec::build::Columns::finish_into(&mut self.$field, out); )*
            }

            fn estimated_bytes(&self) -> usize {
                0 $( + $crate::sec::build::Columns::estimated_bytes(&self.$field) )*
            }
        }
    };
}
pub(crate) use sec_columns;

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

/// The table-level operations the mapper needs, object-safe.
pub(crate) trait SecTable {
    fn name(&self) -> &'static str;
    /// Rows buffered.
    fn len(&self) -> usize;
    /// Logical buffer bytes.
    fn estimated_bytes(&self) -> usize;
    /// The buffered rows as a batch with the table's schema; the builders are
    /// reset (an empty table gives an empty batch with the same schema).
    fn finish(&mut self) -> Result<RecordBatch>;
}

/// One output table: canonical identity, the columns `C`, optional fork step.
pub(crate) struct Table<C> {
    name: &'static str,
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    cols: C,
    fork_step: Option<ForkStepBuilder>,
}

const CANONICAL_COLUMNS: usize = 7;

impl<C: Columns> Table<C> {
    /// Build `table` from its schema (`super::schema::table_schema`).
    ///
    /// # Panics
    /// If `C` does not match the schema's columns exactly.
    pub(crate) fn new(
        table: &'static str,
        include_fork_step: bool,
        encoding: &EncodeBytes,
    ) -> Self {
        let schema = Arc::new(schema::table_schema(table, include_fork_step, encoding));
        let fields = schema.fields();
        let end = fields.len() - if include_fork_step { 2 } else { 0 };
        let mut domain = fields[CANONICAL_COLUMNS..end].iter();
        let cols = C::from_fields(table, &mut domain);
        assert!(
            domain.next().is_none(),
            "sec table `{table}`: the column struct ends before the schema"
        );
        Self {
            name: table,
            schema,
            canonical: CanonicalBuilder::with_encoding(encoding),
            cols,
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    /// Start a row: append its canonical identity and stream event, and return
    /// the table's columns, every one of which the caller must append once.
    pub(crate) fn row(&mut self, ctx: &AppendCtx<'_>) -> &mut C {
        self.canonical.append(ctx.id);
        append_fork_step(&mut self.fork_step, ctx.event);
        &mut self.cols
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<C: Columns> SecTable for Table<C> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn len(&self) -> usize {
        self.canonical.len()
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + self.cols.estimated_bytes()
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        self.cols.finish_into(&mut columns);
        finish_fork_step(&mut self.fork_step, &mut columns);
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .with_context(|| format!("sec table `{}`", self.name))
    }
}

// ---------------------------------------------------------------------------
// Single columns
// ---------------------------------------------------------------------------

macro_rules! single_column {
    ($(#[$meta:meta])* $name:ident($builder:ty), $make:expr, $est:expr) => {
        $(#[$meta])*
        pub(crate) struct $name(pub $builder);

        impl Columns for $name {
            fn from_fields(name: &'static str, fields: &mut FieldIter<'_>) -> Self {
                let field = next_field(name, fields);
                let make: fn(&Field) -> $builder = $make;
                Self(make(field))
            }

            fn finish_into(&mut self, out: &mut Vec<ArrayRef>) {
                out.push(Arc::new(self.0.finish()));
            }

            fn estimated_bytes(&self) -> usize {
                let est: fn(&$builder) -> usize = $est;
                est(&self.0)
            }
        }
    };
}

fn expect_type(field: &Field, expected: &DataType) {
    assert_eq!(
        field.data_type(),
        expected,
        "sec column `{}` has another Arrow type than its builder",
        field.name()
    );
}

single_column!(
    /// `Utf8`.
    Str(StringBuilder),
    |field| {
        expect_type(field, &DataType::Utf8);
        StringBuilder::new()
    },
    est_str
);
single_column!(
    /// `Boolean`.
    Bool(BooleanBuilder),
    |field| {
        expect_type(field, &DataType::Boolean);
        BooleanBuilder::new()
    },
    est_bool
);
single_column!(
    /// `UInt32`.
    U32(UInt32Builder),
    |field| {
        expect_type(field, &DataType::UInt32);
        UInt32Builder::new()
    },
    est_u32
);
single_column!(
    /// `Int32`.
    I32(Int32Builder),
    |field| {
        expect_type(field, &DataType::Int32);
        Int32Builder::new()
    },
    est_i32
);
single_column!(
    /// `Int64`.
    I64(Int64Builder),
    |field| {
        expect_type(field, &DataType::Int64);
        Int64Builder::new()
    },
    est_i64
);
single_column!(
    /// `Date32` (days since 1970-01-01).
    Date(Date32Builder),
    |field| {
        expect_type(field, &DataType::Date32);
        Date32Builder::new()
    },
    est_date32
);
single_column!(
    /// `Timestamp(Millisecond, "UTC")`.
    Ts(TimestampMillisecondBuilder),
    |field| TimestampMillisecondBuilder::new().with_data_type(field.data_type().clone()),
    est_ts_ms
);
single_column!(
    /// `Decimal128(38, s)`; values are mantissas at the column's scale (the
    /// family the value was parsed with).
    Dec(Decimal128Builder),
    |field| {
        assert!(
            matches!(field.data_type(), DataType::Decimal128(38, _)),
            "sec column `{}` is not a Decimal128(38, s)",
            field.name()
        );
        Decimal128Builder::new().with_data_type(field.data_type().clone())
    },
    est_dec128
);
single_column!(
    /// `Dictionary(Int32, Utf8)`: mapper discriminators.
    Dict(StringDictionaryBuilder<Int32Type>),
    |field| {
        expect_type(field, &firehose_parquet::traits::enum_data_type());
        StringDictionaryBuilder::new()
    },
    |b| estimated_dictionary_index_bytes(b.len())
);
single_column!(
    /// `Binary` (raw bytes whatever the encoding).
    Bin(BinaryBuilder),
    |field| {
        expect_type(field, &DataType::Binary);
        BinaryBuilder::new()
    },
    est_bin
);

/// Estimated bytes of a `Decimal128Builder`: 16 per value plus validity.
pub(crate) fn est_dec128(builder: &Decimal128Builder) -> usize {
    let len = builder.len();
    len * 16 + len.div_ceil(8)
}

impl Str {
    /// A proto string: `""` → NULL, otherwise verbatim.
    pub(crate) fn nz(&mut self, value: &str) {
        if value.is_empty() {
            self.0.append_null();
        } else {
            self.0.append_value(value);
        }
    }

    /// A non-null value (`accession_number`, `form_type`).
    pub(crate) fn val(&mut self, value: &str) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<&str>) {
        self.0.append_option(value);
    }

    pub(crate) fn null(&mut self) {
        self.0.append_null();
    }
}

impl Bool {
    pub(crate) fn val(&mut self, value: bool) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<bool>) {
        self.0.append_option(value);
    }
}

impl U32 {
    pub(crate) fn val(&mut self, value: u32) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<u32>) {
        self.0.append_option(value);
    }
}

impl I32 {
    pub(crate) fn val(&mut self, value: i32) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<i32>) {
        self.0.append_option(value);
    }
}

impl I64 {
    pub(crate) fn val(&mut self, value: i64) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<i64>) {
        self.0.append_option(value);
    }
}

impl Date {
    /// `Date32` days.
    pub(crate) fn opt(&mut self, days: Option<i32>) {
        self.0.append_option(days);
    }
}

impl Ts {
    /// Unix milliseconds.
    pub(crate) fn opt(&mut self, millis: Option<i64>) {
        self.0.append_option(millis);
    }
}

impl Dec {
    /// A mantissa at this column's scale.
    pub(crate) fn opt(&mut self, mantissa: Option<i128>) {
        self.0.append_option(mantissa);
    }
}

impl Dict {
    pub(crate) fn val(&mut self, value: &str) {
        self.0.append_value(value);
    }

    pub(crate) fn opt(&mut self, value: Option<&str>) {
        self.0.append_option(value);
    }
}

impl Bin {
    pub(crate) fn val(&mut self, value: &[u8]) {
        self.0.append_value(value);
    }
}

// ---------------------------------------------------------------------------
// List columns
// ---------------------------------------------------------------------------

fn list_item(field: &Field, expected: &DataType) -> FieldRef {
    match field.data_type() {
        DataType::List(item) if item.data_type() == expected => Arc::clone(item),
        other => panic!(
            "sec column `{}` is {other}, not List<{expected}>",
            field.name()
        ),
    }
}

macro_rules! list_column {
    ($(#[$meta:meta])* $name:ident($values:ty), $item:expr, $make:expr, $est:expr) => {
        $(#[$meta])*
        pub(crate) struct $name(pub ListBuilder<$values>);

        impl Columns for $name {
            fn from_fields(name: &'static str, fields: &mut FieldIter<'_>) -> Self {
                let field = next_field(name, fields);
                let make: fn() -> $values = $make;
                Self(ListBuilder::new(make()).with_field(list_item(field, &$item)))
            }

            fn finish_into(&mut self, out: &mut Vec<ArrayRef>) {
                out.push(Arc::new(self.0.finish()));
            }

            fn estimated_bytes(&self) -> usize {
                let est: fn(&$values) -> usize = $est;
                (self.0.len() + 1) * 4 + est(self.0.values_ref())
            }
        }
    };
}

list_column!(
    /// `List<Utf8>`; the column is never NULL (`[]` when empty).
    ListStr(StringBuilder),
    DataType::Utf8,
    StringBuilder::new,
    est_str
);
list_column!(
    /// `List<Date32>`; never NULL, items NULL when they do not parse.
    ListDate(Date32Builder),
    DataType::Date32,
    Date32Builder::new,
    est_date32
);
list_column!(
    /// `List<Int32>`; never NULL, items NULL when they do not parse.
    ListI32(Int32Builder),
    DataType::Int32,
    Int32Builder::new,
    est_i32
);

impl ListStr {
    /// Items verbatim, in order (`""` stays `""`).
    pub(crate) fn items<'x>(&mut self, items: impl IntoIterator<Item = &'x str>) {
        for item in items {
            self.0.values().append_value(item);
        }
        self.0.append(true);
    }

    /// Items in order, `""` → NULL item (`owner_ciks`, `owner_names`).
    pub(crate) fn items_nz<'x>(&mut self, items: impl IntoIterator<Item = &'x str>) {
        for item in items {
            if item.is_empty() {
                self.0.values().append_null();
            } else {
                self.0.values().append_value(item);
            }
        }
        self.0.append(true);
    }
}

impl ListDate {
    pub(crate) fn items(&mut self, items: impl IntoIterator<Item = Option<i32>>) {
        for item in items {
            self.0.values().append_option(item);
        }
        self.0.append(true);
    }
}

impl ListI32 {
    pub(crate) fn items(&mut self, items: impl IntoIterator<Item = Option<i32>>) {
        for item in items {
            self.0.values().append_option(item);
        }
        self.0.append(true);
    }
}

/// One member builder of a [`ListStruct`].
pub(crate) enum MemberBuilder {
    Utf8(StringBuilder),
    Date32(Date32Builder),
}

impl MemberBuilder {
    fn len(&self) -> usize {
        match self {
            MemberBuilder::Utf8(b) => b.len(),
            MemberBuilder::Date32(b) => b.len(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            MemberBuilder::Utf8(b) => Arc::new(b.finish()),
            MemberBuilder::Date32(b) => Arc::new(b.finish()),
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            MemberBuilder::Utf8(b) => est_str(b),
            MemberBuilder::Date32(b) => est_date32(b),
        }
    }
}

/// `List<Struct<…>>` of `Utf8`/`Date32` members; never NULL (`[]` when
/// empty), elements never NULL, members nullable.
///
/// ```ignore
/// for name in &party.former_names {
///     row.former_names.utf8(0).nz(&name.name);      // member 0: name
///     row.former_names.date(1).append_option(days); // member 1: date_changed
///     row.former_names.end_item();
/// }
/// row.former_names.end_row();
/// ```
pub(crate) struct ListStruct {
    item: FieldRef,
    members: Fields,
    builders: Vec<MemberBuilder>,
    offsets: Vec<i32>,
}

impl Columns for ListStruct {
    fn from_fields(name: &'static str, fields: &mut FieldIter<'_>) -> Self {
        let field = next_field(name, fields);
        let item = match field.data_type() {
            DataType::List(item) => Arc::clone(item),
            other => panic!("sec column `{name}` is {other}, not List<Struct>"),
        };
        let members = match item.data_type() {
            DataType::Struct(members) => members.clone(),
            other => panic!("sec column `{name}` is List<{other}>, not List<Struct>"),
        };
        let builders = members
            .iter()
            .map(|member| match member.data_type() {
                DataType::Utf8 => MemberBuilder::Utf8(StringBuilder::new()),
                DataType::Date32 => MemberBuilder::Date32(Date32Builder::new()),
                other => panic!("sec column `{name}`: unsupported member type {other}"),
            })
            .collect();
        Self {
            item,
            members,
            builders,
            offsets: vec![0],
        }
    }

    fn finish_into(&mut self, out: &mut Vec<ArrayRef>) {
        let children: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(MemberBuilder::finish)
            .collect();
        let structs = StructArray::new(self.members.clone(), children, None);
        let offsets = OffsetBuffer::new(std::mem::replace(&mut self.offsets, vec![0]).into());
        let list = ListArray::new(
            Arc::clone(&self.item),
            offsets,
            Arc::new(structs),
            None::<NullBuffer>,
        );
        out.push(Arc::new(list));
    }

    fn estimated_bytes(&self) -> usize {
        self.offsets.len() * 4
            + self
                .builders
                .iter()
                .map(MemberBuilder::estimated_bytes)
                .sum::<usize>()
    }
}

impl ListStruct {
    /// The `Utf8` builder of member `index` (schema member order).
    pub(crate) fn utf8(&mut self, index: usize) -> &mut StringBuilder {
        match &mut self.builders[index] {
            MemberBuilder::Utf8(b) => b,
            MemberBuilder::Date32(_) => panic!("member {index} is Date32"),
        }
    }

    /// The `Date32` builder of member `index` (schema member order).
    pub(crate) fn date(&mut self, index: usize) -> &mut Date32Builder {
        match &mut self.builders[index] {
            MemberBuilder::Date32(b) => b,
            MemberBuilder::Utf8(_) => panic!("member {index} is Utf8"),
        }
    }

    /// End one struct element; every member must have been appended once.
    pub(crate) fn end_item(&mut self) {
        debug_assert!(
            {
                let items = self.builders.first().map_or(0, MemberBuilder::len);
                self.builders.iter().all(|b| b.len() == items)
            },
            "every List<Struct> member must be appended once per element"
        );
    }

    /// End the row's list.
    pub(crate) fn end_row(&mut self) {
        let items = self.builders.first().map_or(0, MemberBuilder::len);
        self.offsets
            .push(i32::try_from(items).expect("List<Struct> offsets fit i32"));
    }

    /// One row whose elements are all-`Utf8` members, `""` → NULL member
    /// (`other_identifiers`, `explanatory_notes`, `series_reports`, …): each
    /// element gives its members in schema order.
    pub(crate) fn utf8_items<'x, const N: usize>(
        &mut self,
        items: impl IntoIterator<Item = [&'x str; N]>,
    ) {
        debug_assert_eq!(self.builders.len(), N);
        for item in items {
            for (index, value) in item.into_iter().enumerate() {
                let builder = self.utf8(index);
                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value);
                }
            }
            self.end_item();
        }
        self.end_row();
    }
}

// ---------------------------------------------------------------------------
// Column groups
// ---------------------------------------------------------------------------

sec_columns! {
    /// The filing context [FC] (§3.0): `filing_index`, `accession_number`,
    /// `form_type`, `filing_date`, `acceptance_datetime`.
    pub(crate) struct Fc {
        pub filing_index: U32,
        pub accession_number: Str,
        pub form_type: Str,
        pub filing_date: Date,
        pub acceptance_datetime: Ts,
    }
}

impl Fc {
    pub(crate) fn append(&mut self, fc: &FilingCtx<'_>) {
        self.filing_index.val(fc.filing_index);
        self.accession_number.val(fc.accession_number);
        self.form_type.val(fc.form_type);
        self.filing_date.opt(fc.filing_date);
        self.acceptance_datetime.opt(fc.acceptance_ms);
    }
}

/// An `Address` flattened into 8 nullable `Utf8` columns named after the struct
/// member: `<prefix>_street1`, `_street2`, `_city`, `_state`, `_zip_code`,
/// `_state_description`, `_country`, `_non_us_state_territory` (§3.0).
pub(crate) struct Addr {
    pub street1: Str,
    pub street2: Str,
    pub city: Str,
    pub state: Str,
    pub zip_code: Str,
    pub state_description: Str,
    pub country: Str,
    pub non_us_state_territory: Str,
}

/// The `Address` member suffixes, in column order.
pub(crate) const ADDRESS_FIELDS: [&str; 8] = [
    "street1",
    "street2",
    "city",
    "state",
    "zip_code",
    "state_description",
    "country",
    "non_us_state_territory",
];

impl Columns for Addr {
    fn from_fields(prefix: &'static str, fields: &mut FieldIter<'_>) -> Self {
        let mut next = |suffix: &str| {
            let field = fields
                .next()
                .unwrap_or_else(|| panic!("sec address `{prefix}` has no schema field left"));
            assert_eq!(
                field.name(),
                &format!("{prefix}_{suffix}"),
                "sec address columns differ from the schema"
            );
            expect_type(field, &DataType::Utf8);
            Str(StringBuilder::new())
        };
        Self {
            street1: next(ADDRESS_FIELDS[0]),
            street2: next(ADDRESS_FIELDS[1]),
            city: next(ADDRESS_FIELDS[2]),
            state: next(ADDRESS_FIELDS[3]),
            zip_code: next(ADDRESS_FIELDS[4]),
            state_description: next(ADDRESS_FIELDS[5]),
            country: next(ADDRESS_FIELDS[6]),
            non_us_state_territory: next(ADDRESS_FIELDS[7]),
        }
    }

    fn finish_into(&mut self, out: &mut Vec<ArrayRef>) {
        for column in self.columns_mut() {
            column.finish_into(out);
        }
    }

    fn estimated_bytes(&self) -> usize {
        [
            &self.street1,
            &self.street2,
            &self.city,
            &self.state,
            &self.zip_code,
            &self.state_description,
            &self.country,
            &self.non_us_state_territory,
        ]
        .into_iter()
        .map(Columns::estimated_bytes)
        .sum()
    }
}

impl Addr {
    fn columns_mut(&mut self) -> [&mut Str; 8] {
        [
            &mut self.street1,
            &mut self.street2,
            &mut self.city,
            &mut self.state,
            &mut self.zip_code,
            &mut self.state_description,
            &mut self.country,
            &mut self.non_us_state_territory,
        ]
    }

    /// Append an optional `Address`: every member verbatim with `""` → NULL,
    /// all 8 NULL when the message is absent.
    pub(crate) fn append(&mut self, address: Option<&sec::Address>) {
        match address {
            Some(a) => {
                self.street1.nz(&a.street1);
                self.street2.nz(&a.street2);
                self.city.nz(&a.city);
                self.state.nz(&a.state);
                self.zip_code.nz(&a.zip_code);
                self.state_description.nz(&a.state_description);
                self.country.nz(&a.country);
                self.non_us_state_territory.nz(&a.non_us_state_territory);
            }
            None => {
                for column in self.columns_mut() {
                    column.null();
                }
            }
        }
    }
}

/// The `{description, value}` members of an `OtherIdentifier` list
/// (`List<Struct<description: Utf8, value: Utf8>>`).
pub(crate) fn other_identifier_items(
    items: &[sec::OtherIdentifier],
) -> impl Iterator<Item = [&str; 2]> {
    items
        .iter()
        .map(|item| [item.description.as_str(), item.value.as_str()])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Date32Type;

    #[test]
    fn list_struct_round_trips_members_and_empty_rows() {
        let field = Arc::new(Field::new(
            "former_names",
            schema::Ty::ListStruct(&[
                ("name", schema::Member::Utf8),
                ("date_changed", schema::Member::Date32),
            ])
            .data_type(),
            false,
        ));
        let fields = [field];
        let mut builder = ListStruct::from_fields("former_names", &mut fields.iter());
        builder.utf8(0).append_value("OLD NAME");
        builder.date(1).append_value(10);
        builder.end_item();
        builder.utf8(0).append_null();
        builder.date(1).append_null();
        builder.end_item();
        builder.end_row();
        builder.end_row();
        let mut out = Vec::new();
        builder.finish_into(&mut out);
        let list = out[0].as_list::<i32>();
        assert_eq!(list.data_type(), fields[0].data_type());
        assert_eq!(list.len(), 2);
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 0);
        let first = list.value(0);
        let structs = first.as_struct();
        assert_eq!(structs.column(0).as_string::<i32>().value(0), "OLD NAME");
        assert!(structs.column(0).is_null(1));
        assert_eq!(structs.column(1).as_primitive::<Date32Type>().value(0), 10);

        // The builder is reusable after finish.
        builder.end_row();
        let mut out = Vec::new();
        builder.finish_into(&mut out);
        assert_eq!(out[0].len(), 1);
        assert_eq!(out[0].data_type(), fields[0].data_type());
    }

    #[test]
    #[should_panic(expected = "column order differs from the schema")]
    fn a_column_struct_that_drifts_from_its_schema_panics() {
        sec_columns! {
            struct Wrong {
                feed_date: Date,
                has_parse_issues: Bool,
            }
        }
        let _ = Table::<Wrong>::new(schema::BLOCKS, false, &EncodeBytes::Hex);
    }
}
