use pgx::{Datum, PgOid, SpiClient, SpiTupleTable};
use std::ops::{Deref, DerefMut};
use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::catch_error::*;
use crate::subtxn::*;

use crate::error::{Error, PostgresError};

/// Read-only commands for SPI interface
pub trait CheckedCommands {
    type Result<A>;

    /// Execute a read-only command, returning an error if one occurred.
    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError>;
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
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError>;
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
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
        catch_error(self, |xact| (xact.select(query, limit, args), xact))
            .map_err(Error::into_postgres_error)
    }
}

impl<Parent: DerefMut<Target = SpiClient> + UnwindSafe + RefUnwindSafe> CheckedMutCommands
    for SubTransaction<Parent>
{
    type Result<A> = (A, SubTransaction<Parent>);

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
        catch_error(self, |mut xact| (xact.update(query, limit, args), xact))
            .map_err(Error::into_postgres_error)
    }
}

impl CheckedCommands for SpiClient {
    type Result<A> = (A, Self);

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
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
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
        // Here we rely on the fact that `SpiClient` can be created at any time. This may not hold true in the future
        // However, we need the client to be consumed by `sub_transaction`, so we do this for now.
        SpiClient
            .sub_transaction(|xact| xact.checked_select(query, limit, args))
            .map(|(table, _xact)| table)
    }
}

impl CheckedMutCommands for SpiClient {
    type Result<A> = (A, Self);

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
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
    ) -> Result<Self::Result<SpiTupleTable>, PostgresError> {
        // Here we rely on the fact that `SpiClient` can be created at any time. This may not hold true in the future
        // However, we need the client to be consumed by `sub_transaction`, so we do this for now.
        SpiClient
            .sub_transaction(|xact| xact.checked_update(query, limit, args))
            .map(|(table, _xact)| table)
    }
}
