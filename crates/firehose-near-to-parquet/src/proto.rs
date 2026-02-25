pub mod sf {
    pub mod near {
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.near.r#type.v1.rs"));
            }
        }
    }
}

pub use sf::near::r#type::v1 as near;
