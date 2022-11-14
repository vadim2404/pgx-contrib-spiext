use pgx::pg_sys::panic::CaughtError;
use pgx::PgTryBuilder;
use pgx::{pg_sys::Datum, PgOid, SpiClient, SpiTupleTable};
use std::ops::{Deref, DerefMut};
use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::subtxn::*;

/// Read-only commands for SPI interface
pub trait CheckedCommands {
    type Result<A>;

    /// Execute a read-only command, returning an error if one occurred.
    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError>;
}

/// Mutable commands for SPI interface
pub trait CheckedMutCommands {
    type Result<A>;

    /// Execute a mutable command, returning an error if one occurred.
    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError>;
}

impl<Parent: Deref<Target = SpiClient> + UnwindSafe + RefUnwindSafe> CheckedCommands
    for SubTransaction<Parent>
{
    type Result<A> = (A, SubTransaction<Parent>);

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        PgTryBuilder::new(move || Ok((self.select(query, limit, args), self)))
            .catch_others(|e| Err(e))
            .execute()
    }
}

impl<Parent: DerefMut<Target = SpiClient> + UnwindSafe + RefUnwindSafe> CheckedMutCommands
    for SubTransaction<Parent>
{
    type Result<A> = (A, SubTransaction<Parent>);

    fn checked_update(
        mut self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        PgTryBuilder::new(move || Ok((self.update(query, limit, args), self)))
            .catch_others(|e| Err(e))
            .execute()
    }
}

impl CheckedCommands for SpiClient {
    type Result<A> = (A, Self);

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.sub_transaction(|xact| xact.checked_select(query, limit, args))
            .map(|(table, xact)| (table, xact.commit().into_inner()))
    }
}

impl<'a> CheckedCommands for &'a SpiClient {
    type Result<A> = A;

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        // Here we rely on the fact that `SpiClient` can be created at any time. This may not hold true in the future
        // However, we need the client to be consumed by `sub_transaction`, so we do this for now.
        SpiClient
            .sub_transaction(|xact| xact.checked_select(query, limit, args))
            .map(|(table, xact)| {
                xact.commit();
                table
            })
    }
}

impl CheckedMutCommands for SpiClient {
    type Result<A> = (A, Self);

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.sub_transaction(|xact| xact.checked_update(query, limit, args))
            .map(|(table, xact)| (table, xact.commit().into_inner()))
    }
}

impl<'a> CheckedMutCommands for &'a mut SpiClient {
    type Result<A> = A;

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        // Here we rely on the fact that `SpiClient` can be created at any time. This may not hold true in the future
        // However, we need the client to be consumed by `sub_transaction`, so we do this for now.
        SpiClient
            .sub_transaction(|xact| xact.checked_update(query, limit, args))
            .map(|(table, xact)| {
                xact.commit();
                table
            })
    }
}
