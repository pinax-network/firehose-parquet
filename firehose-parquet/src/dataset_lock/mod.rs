//! Exclusive ownership shared by dataset mutations.
//!
//! These guards coordinate cooperating fireparq processes. They do not fence
//! older binaries, external tools, or administrators replacing directory trees.

mod local;
mod operation;
pub(crate) mod session;

pub use local::LocalOwnership;
pub use operation::{DatasetOwnership, MutationScope};
