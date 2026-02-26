/// Compiled protobuf definitions for all Firehose chain types.

// Firehose core
pub mod sf {
    pub mod firehose {
        pub mod v2 {
            tonic::include_proto!("sf.firehose.v2");
        }
    }

    pub mod solana {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.solana.r#type.v1.rs"));
            }
        }
    }

    pub mod ethereum {
        #[path = ""]
        pub mod r#type {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/sf.ethereum.r#type.v2.rs"));
            }
        }
    }

    pub mod bitcoin {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.bitcoin.r#type.v1.rs"));
            }
        }
    }

    pub mod near {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.near.r#type.v1.rs"));
            }
        }
    }

    pub mod antelope {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.antelope.r#type.v1.rs"));
            }
        }
    }

    pub mod cosmos {
        #[path = ""]
        pub mod r#type {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/sf.cosmos.r#type.v2.rs"));
            }
        }
    }

    pub mod tron {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.tron.r#type.v1.rs"));
            }
        }
    }

    pub mod beacon {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.beacon.r#type.v1.rs"));
            }
        }
    }
}

// Cosmos SDK types
pub mod cosmos_sdk {
    pub mod tx {
        pub mod v1beta1 {
            include!(concat!(env!("OUT_DIR"), "/cosmos.tx.v1beta1.rs"));
        }
    }
}

// Tron protocol types
pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

// Convenience aliases
pub use sf::firehose::v2 as firehose;
pub use sf::solana::r#type::v1 as solana;
pub use sf::ethereum::r#type::v2 as eth;
pub use sf::bitcoin::r#type::v1 as btc;
pub use sf::near::r#type::v1 as near;
pub use sf::antelope::r#type::v1 as antelope;
pub use sf::cosmos::r#type::v2 as cosmos;
pub use sf::tron::r#type::v1 as tron;
pub use sf::beacon::r#type::v1 as beacon;
pub use cosmos_sdk::tx::v1beta1 as cosmos_tx;
