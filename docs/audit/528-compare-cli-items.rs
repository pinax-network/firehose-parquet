use quote::ToTokens;
use serde_json::json;
use std::{collections::BTreeMap, path::Path};
use syn::{visit_mut::VisitMut, Item, Visibility};
struct Normalize;
impl VisitMut for Normalize {
    fn visit_signature_mut(&mut self, value: &mut syn::Signature) {
        syn::visit_mut::visit_signature_mut(self, value);
        value.inputs = value.inputs.clone().into_iter().collect();
    }
    fn visit_generics_mut(&mut self, value: &mut syn::Generics) {
        syn::visit_mut::visit_generics_mut(self, value);
        value.params = value.params.clone().into_iter().collect();
    }
    fn visit_expr_call_mut(&mut self, value: &mut syn::ExprCall) {
        syn::visit_mut::visit_expr_call_mut(self, value);
        value.args = value.args.clone().into_iter().collect();
    }
    fn visit_expr_method_call_mut(&mut self, value: &mut syn::ExprMethodCall) {
        syn::visit_mut::visit_expr_method_call_mut(self, value);
        value.args = value.args.clone().into_iter().collect();
    }
    fn visit_macro_mut(&mut self, value: &mut syn::Macro) {
        if value.path.is_ident("vec") {
            let tokens = &value.tokens;
            let mut array: syn::Expr = syn::parse2(quote::quote!([#tokens])).expect("vec expression array");
            self.visit_expr_mut(&mut array);
            let stream = array.to_token_stream();
            let Some(proc_macro2::TokenTree::Group(group)) = stream.into_iter().next() else { panic!("array group") };
            value.tokens = group.stream();
        }
    }
    fn visit_visibility_mut(&mut self, visibility: &mut Visibility) {
        if let Visibility::Restricted(r) = visibility {
            if r.path.to_token_stream().to_string() == "crate :: cli" {
                *visibility = Visibility::Inherited;
            }
        }
    }
}
fn collect(path: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<String>>) {
    let mut parsed = syn::parse_file(&std::fs::read_to_string(path).unwrap()).unwrap();
    Normalize.visit_file_mut(&mut parsed);
    for item in parsed.items { collect_item(item, prefix, out); }
}
fn collect_item(item: Item, prefix: &str, out: &mut BTreeMap<String, Vec<String>>) {
    let key = match &item {
        Item::Use(_) => return,
        Item::Mod(x) => {
            if let Some((_, items)) = &x.content {
                for item in items { collect_item(item.clone(), &format!("{prefix}{}::",x.ident), out); }
            }
            return;
        },
        Item::Fn(x) => format!("fn {}",x.sig.ident),
        Item::Struct(x) => format!("struct {}",x.ident),
        Item::Enum(x) => format!("enum {}",x.ident),
        Item::Type(x) => format!("type {}",x.ident),
        Item::Const(x) => format!("const {}",x.ident),
        Item::Impl(x) => format!("impl {} {}",x.self_ty.to_token_stream(),x.trait_.as_ref().map(|(_,p,_)|p.to_token_stream().to_string()).unwrap_or_default()),
        other => panic!("unsupported item {}",other.to_token_stream()),
    };
    out.entry(format!("{prefix}{key}")).or_default().push(item.to_token_stream().to_string());
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let mut before = BTreeMap::new(); let mut after = BTreeMap::new();
    collect(Path::new(&args[1]), "", &mut before);
    for path in &args[2..] {
        let prefix = if path.ends_with("/tests.rs") {"tests::"} else {""};
        collect(Path::new(path), prefix, &mut after);
    }
    for values in before.values_mut().chain(after.values_mut()) { values.sort(); }
    

    if before != after {
        for key in before.keys().chain(after.keys()) {
            if before.get(key) != after.get(key) { eprintln!("DIFF {key}"); }
        }
        std::process::exit(1);
    }
    println!("{}",serde_json::to_string_pretty(&json!({
        "baseline":"270af1674597da4e5dde9433ee672c2c5ebfb1a2", "same":true,
        "normalization":"Rust syn/quote item tokens; only newly restricted pub(in crate::cli) normalized to inherited; rustfmt-only trailing commas in function/generic/call argument lists (including vec expression arrays) normalized; use/module declarations excluded, inline tests flattened; executable bodies, signatures, types, values and attributes unchanged",
        "item_count":before.values().map(Vec::len).sum::<usize>(),
        "items":before.keys().collect::<Vec<_>>()
    })).unwrap());
}
