use pgx::{pg_sys, PgMemoryContexts, SpiClient};
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

/// Sub-transaction
///
/// Unless rolled back or committed explicitly, it'll commit if `COMMIT` generic parameter is `true`
/// (default) or roll back if it is `false`.
pub struct SubTransaction<Parent, const COMMIT: bool = true> {
    memory_context: pg_sys::MemoryContext,
    resource_owner: pg_sys::ResourceOwner,
    // Should the the transaction be dropped, or was it already
    // committed or rolled back? True if it should be dropped.
    drop: bool,
    parent: Option<Parent>,
}

impl<Parent, const COMMIT: bool> Debug for SubTransaction<Parent, COMMIT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(std::any::type_name::<Self>())
    }
}

impl<Parent, const COMMIT: bool> SubTransaction<Parent, COMMIT> {
    /// Create a new sub-transaction.
    ///
    /// Can be only used by this crate.
    fn new(parent: Parent) -> Self {
        // Remember the memory context before starting the sub-transaction
        let ctx = PgMemoryContexts::CurrentMemoryContext.value();
        // Remember resource owner before starting the sub-transaction
        let resource_owner = unsafe { pg_sys::CurrentResourceOwner };
        unsafe {
            pg_sys::BeginInternalSubTransaction(std::ptr::null());
        }
        // Switch to the outer memory context so that all allocations remain
        // there instead of the sub-transaction's context
        PgMemoryContexts::For(ctx).set_as_current();
        Self {
            memory_context: ctx,
            drop: true,
            resource_owner,
            parent: Some(parent),
        }
    }

    /// Commit the transaction, returning its parent
    pub fn commit(mut self) -> Parent {
        self.internal_commit();
        self.drop = false;
        self.parent.take().unwrap()
    }

    /// Rollback the transaction, returning its parent
    pub fn rollback(mut self) -> Parent {
        self.internal_rollback();
        self.drop = false;
        self.parent.take().unwrap()
    }

    /// Returns the memory context this transaction is in
    pub fn memory_context(&self) -> PgMemoryContexts {
        PgMemoryContexts::For(self.memory_context)
    }

    fn internal_rollback(&self) {
        unsafe {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::CurrentResourceOwner = self.resource_owner;
        }
        PgMemoryContexts::For(self.memory_context).set_as_current();
    }

    fn internal_commit(&self) {
        unsafe {
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::CurrentResourceOwner = self.resource_owner;
        }
        PgMemoryContexts::For(self.memory_context).set_as_current();
    }
}

impl<Parent> SubTransaction<Parent, true> {
    /// Make this sub-transaction roll back on drop
    pub fn rollback_on_drop(self) -> SubTransaction<Parent, false> {
        self.into()
    }
}

impl<Parent> SubTransaction<Parent, false> {
    /// Make this sub-transaction commit on drop
    pub fn commit_on_drop(self) -> SubTransaction<Parent, true> {
        self.into()
    }
}

impl<Parent> Into<SubTransaction<Parent, false>> for SubTransaction<Parent, true> {
    fn into(mut self) -> SubTransaction<Parent, false> {
        let result = SubTransaction {
            memory_context: self.memory_context,
            resource_owner: self.resource_owner,
            drop: self.drop,
            parent: self.parent.take(),
        };
        // Make sure original sub-transaction won't commit
        self.drop = false;
        result
    }
}

impl<Parent> Into<SubTransaction<Parent, true>> for SubTransaction<Parent, false> {
    fn into(mut self) -> SubTransaction<Parent, true> {
        let result = SubTransaction {
            memory_context: self.memory_context,
            resource_owner: self.resource_owner,
            drop: self.drop,
            parent: self.parent.take(),
        };
        // Make sure original sub-transaction won't roll back
        self.drop = false;
        result
    }
}

impl<Parent, const COMMIT: bool> Drop for SubTransaction<Parent, COMMIT> {
    fn drop(&mut self) {
        if self.drop {
            if COMMIT {
                self.internal_commit();
            } else {
                self.internal_rollback();
            }
        }
    }
}

impl<Parent, const COMMIT: bool> Deref for SubTransaction<Parent, COMMIT> {
    type Target = Parent;

    fn deref(&self) -> &Self::Target {
        self.parent.as_ref().unwrap()
    }
}

impl<Parent, const COMMIT: bool> DerefMut for SubTransaction<Parent, COMMIT> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parent.as_mut().unwrap()
    }
}

/// An internal `SpiClient` wrapper for typing purposes
pub struct SpiClientWrapper(SpiClient);

impl SpiClientWrapper {
    pub(crate) fn into_inner(self) -> SpiClient {
        self.0
    }
}

impl Deref for SpiClientWrapper {
    type Target = SpiClient;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SpiClientWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Trait that allows creating a sub_transaction off any type
pub trait SubTransactionExt {
    /// Parent's type
    type T;

    /// Consume `self` and return a sub-transaction
    fn sub_transaction<F: FnOnce(SubTransaction<Self::T>) -> R, R>(self, f: F) -> R
    where
        Self: Sized;
}

impl SubTransactionExt for SpiClient {
    type T = SpiClientWrapper;
    fn sub_transaction<F: FnOnce(SubTransaction<Self::T>) -> R, R>(self, f: F) -> R
    where
        Self: Sized,
    {
        let sub_xact = SubTransaction::new(SpiClientWrapper(self));
        f(sub_xact)
    }
}

impl<Parent> SubTransactionExt for SubTransaction<Parent> {
    type T = SubTransaction<Parent>;
    fn sub_transaction<F: FnOnce(SubTransaction<Self::T>) -> R, R>(self, f: F) -> R
    where
        Self: Sized,
    {
        let sub_xact = SubTransaction::new(self);
        f(sub_xact)
    }
}
