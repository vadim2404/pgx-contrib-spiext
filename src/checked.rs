use pgx::pg_sys::panic::CaughtError;
use pgx::PgTryBuilder;
use pgx::{pg_sys::Datum, PgOid, SpiClient, SpiTupleTable};
use std::ops::Deref;
use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::subtxn::*;

/// Commands for SPI interface
pub trait CheckedCommands {
    type Result<A>;

    /// Execute a read-only command, returning an error if one occurred.
    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError>;

    /// Execute a mutable command, returning an error if one occurred.
    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError>;
}

impl<'a, Parent: Deref<Target = SpiClient<'a>> + UnwindSafe + RefUnwindSafe> CheckedCommands
    for SubTransaction<Parent, false>
{
    type Result<A> = (A, SubTransaction<Parent, false>);

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

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        PgTryBuilder::new(move || Ok((self.update(query, limit, args), self)))
            .catch_others(|e| Err(e))
            .execute()
    }
}

impl<'a, Parent: Deref<Target = SpiClient<'a>> + UnwindSafe + RefUnwindSafe> CheckedCommands
    for SubTransaction<Parent, true>
{
    type Result<A> = (A, SubTransaction<Parent, true>);

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.rollback_on_drop()
            .checked_select(query, limit, args)
            .map(|(res, xact)| (res, xact.commit_on_drop()))
    }

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.rollback_on_drop()
            .checked_update(query, limit, args)
            .map(|(res, xact)| (res, xact.commit_on_drop()))
    }
}

impl<'a> CheckedCommands for SpiClient<'a> {
    type Result<A> = (A, Self);

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.sub_transaction(|xact| xact.checked_select(query, limit, args))
            .map(|(table, xact)| (table, *xact.commit()))
    }

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        self.sub_transaction(|xact| xact.checked_update(query, limit, args))
            .map(|(table, xact)| (table, *xact.commit()))
    }
}

impl<'a, 'b: 'a> CheckedCommands for &'a SpiClient<'b> {
    type Result<A> = A;

    fn checked_select(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        let client: SpiClientHolder = self.into();
        client
            .sub_transaction(|xact| xact.checked_select(query, limit, args))
            .map(|(table, _xact): (_, SubTransaction<_, true>)| table)
    }

    fn checked_update(
        self,
        query: &str,
        limit: Option<i64>,
        args: Option<Vec<(PgOid, Option<Datum>)>>,
    ) -> Result<Self::Result<SpiTupleTable>, CaughtError> {
        let client: SpiClientHolder = self.into();
        client
            .sub_transaction(|xact| xact.checked_update(query, limit, args))
            .map(|(table, _xact): (_, SubTransaction<_, true>)| table)
    }
}
