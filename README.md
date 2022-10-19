# Extensions for [pgx](https://crates.io/crates/pgx) SPI API

*Please note that this crate is experimental and may undergo significant changes*

## MSRV

Rust 1.65 (currently in beta, due to be released in November 2022) due to its use of GAT.

## Running tests

Assuming `pgx` is configured with `cargo pgx init`, run `cargo pgx test` from `tests` directory.

## Extensions

### Sub-transactions

Sub-transaction API allows more granular control over data mutations int the database using Postgres sub-transaction
facilities.

### Checked Commands

Checked commands allow to run a SQL comamnd (a query or an update), capturing an error that may have occurred. Pgx
currently does not allow easy access to it. This feature is using PG_CATCH-like approach used in cases where such
capture is
necessary.

## Examples

For examples, please refer to the `tests` directory. 
Once the API will mature a little bit more, examples will be published in the README as well.