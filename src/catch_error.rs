//! # Error-catching
//!
//! Enables catching Rust and (most importantly) Postgres-originating errors.
//!
//! This functionality is not accessible to end-users without opting in with the `pub_catch_error`
//! feature gate. The reason for this is that it use may be brittle as we're dealing with low-level
//! stuff. If it will eventually be proven to be safe, this restriction may be removed.
use crate::error::{Error, PostgresError};
use crate::subtxn::SubTransaction;
use pgx::{pg_sys, JumpContext, PgMemoryContexts};
use std::panic::{catch_unwind, RefUnwindSafe, UnwindSafe};

impl<Parent> SubTransaction<Parent> {
    /// Internal hack to keep a reference to the transaction that goes
    /// into the closure passed to `catch_error` in case if we need to roll it back.
    fn internal_clone(&mut self) -> SubTransaction<()> {
        // Don't drop the original subtxn (equates to committing it) it while it's being used so that
        // we can roll it back. Very important!
        let drop = self.drop;
        self.drop = false;
        SubTransaction {
            memory_context: self.memory_context,
            resource_owner: self.resource_owner,
            // Save original transation's `drop` flag
            drop,
            parent: Some(()),
        }
    }
}

/// Run a closure within a sub-transaction. Rolls the sub-transaction back if any panic occurs
/// and returns the captured error.
///
/// At this moment, this function is internal to `pgx-contrib-spiext`, unless the `pub_catch_error`
/// feature is enabled. This is done to potential safety risks use of this function may bring.
/// If it will eventually be proven to be safe, this restriction may be removed.
pub fn catch_error<Try, R, Parent>(
    mut subtxn: SubTransaction<Parent>,
    try_func: Try,
) -> Result<(R, SubTransaction<Parent>), Error>
where
    Parent: UnwindSafe + RefUnwindSafe,
    Try: FnOnce(SubTransaction<Parent>) -> (R, SubTransaction<Parent>) + UnwindSafe + RefUnwindSafe,
{
    // This is an internal reference to the transaction that we use to roll the transaction
    // back if a panic occurs.
    let mut subtxn_ = subtxn.internal_clone();

    // Run the closure and catch a panic.
    let result = catch_unwind(|| try_func(subtxn));

    match result {
        Ok((result, mut xact)) => {
            // Restore original transaction's `drop` flag
            xact.drop = subtxn_.drop;

            // Ensure we'll NOT drop (meaning commit) the internal clone. Also very important!
            subtxn_.drop = false;
            Ok((result, xact))
        }
        Err(e) => {
            if e.downcast_ref::<JumpContext>().is_some() {
                // Switch to sub-transaction's memory context as we're in error
                // context right now. It will be thrown out when the transaction is aborted below.
                PgMemoryContexts::CurTransactionContext.set_as_current();

                // Copy the error data
                let error_data = unsafe { pg_sys::CopyErrorData() };

                // Prepare the error
                let err = Error::PG(PostgresError::from(unsafe { &*error_data }));

                // Clear the error
                unsafe { pg_sys::FlushErrorState() };

                // Rollback the transaction. Very important to do so!
                subtxn_.rollback();

                Err(err)
            } else {
                Err(Error::Rust(e))
            }
        }
    }
}
