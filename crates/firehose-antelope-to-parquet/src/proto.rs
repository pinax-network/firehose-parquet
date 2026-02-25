pub mod sf {
    pub mod antelope {
        #[path = ""]
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.antelope.r#type.v1.rs"));
            }
        }
    }
}

pub use sf::antelope::r#type::v1 as antelope;
