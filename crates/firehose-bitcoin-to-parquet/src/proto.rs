pub mod sf {
    pub mod bitcoin {
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.bitcoin.r#type.v1.rs"));
            }
        }
    }
}

pub use sf::bitcoin::r#type::v1 as btc;
