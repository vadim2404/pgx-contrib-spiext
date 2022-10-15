//! # Extended functionality for pgx's SPI interface
//!
//! To include all its functionality, include the contents of the `prelude` module:
//!
//! ```rust
//! use pgx_contrib_spiext::prelude::*;
//! ```

#[cfg(feature = "pub_catch_error")]
pub mod catch_error;
#[cfg(not(feature = "pub_catch_error"))]
pub(crate) mod catch_error;
pub mod checked;
pub mod error;
pub mod subtxn;

pub mod prelude {
    pub use crate::checked::*;
    pub use crate::error::*;
    pub use crate::subtxn::*;
}
