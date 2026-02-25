pub mod sf {
    pub mod beacon {
        pub mod r#type {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/sf.beacon.r#type.v1.rs"));
            }
        }
    }
}

pub use sf::beacon::r#type::v1 as beacon;
