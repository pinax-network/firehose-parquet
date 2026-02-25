pub mod sf {
    pub mod ethereum {
        pub mod r#type {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/sf.ethereum.r#type.v2.rs"));
            }
        }
    }
}

pub use sf::ethereum::r#type::v2 as eth;
