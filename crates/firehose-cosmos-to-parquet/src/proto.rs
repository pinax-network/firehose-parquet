pub mod sf {
    pub mod cosmos {
        pub mod r#type {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/sf.cosmos.r#type.v2.rs"));
            }
        }
    }
}

pub mod cosmos_sdk {
    pub mod tx {
        pub mod v1beta1 {
            include!(concat!(env!("OUT_DIR"), "/cosmos.tx.v1beta1.rs"));
        }
    }
}

pub use sf::cosmos::r#type::v2 as cosmos;
pub use cosmos_sdk::tx::v1beta1 as cosmos_tx;
