//! # Extended functionality for pgx's SPI interface
//!
//! To include all its functionality, include the contents of the `prelude` module:
//!
//! ```rust
//! use pgx_contrib_spiext::prelude::*;
//! ```

pub mod checked;
pub mod subtxn;

pub mod prelude {
    pub use crate::checked::*;
    pub use crate::subtxn::*;
}
