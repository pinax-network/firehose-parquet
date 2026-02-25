pub mod sf {
    pub mod solana {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.solana.r#type.v1.rs"));
            }
        }
    }
}

pub use sf::solana::r#type::v1 as solana;
