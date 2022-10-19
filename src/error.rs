use pgx::cstr_core::CStr;
use pgx::log::PgLogLevel;
use pgx::{pg_sys, PgMemoryContexts};
use std::any::Any;
use std::panic::resume_unwind;

/// Postrgres or Rust-originating error
pub enum Error {
    /// Postgres-originating error
    PG(PostgresError),
    /// Rust-originating error
    Rust(Box<dyn Any + Send + 'static>),
}

impl Error {
    /// Returns [`PostgresError`] if `Error` is `Error::PG`,  propagates Rust's panic otherwise.
    pub fn into_postgres_error(self) -> PostgresError {
        match self {
            Error::PG(err) => err,
            Error::Rust(err) => {
                resume_unwind(err);
            }
        }
    }
}

/// Postgres-originating error
pub struct PostgresError {
    pub elevel: PgLogLevel,
    pub output_to_server: bool,
    pub output_to_client: bool,
    #[cfg(not(feature = "pg14"))]
    pub show_funcname: bool,
    pub hide_stmt: bool,
    pub hide_ctx: bool,
    pub filename: Option<String>,
    pub lineno: usize,
    pub funcname: Option<String>,
    pub domain: Option<String>,
    pub context_domain: Option<String>,
    pub sqlerrcode: usize, // TODO: PgSqlErrorCode
    pub message: Option<String>,
    pub detail: Option<String>,
    pub detail_log: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    pub backtrace: Option<String>,
    pub message_id: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub datatype_name: Option<String>,
    pub constraint_name: Option<String>,
    pub cursorpos: usize,
    pub internalpos: usize,
    pub internalquery: Option<String>,
    pub saved_errno: usize,
    pub assoc_context: PgMemoryContexts,
}

impl<'a> From<&'a pg_sys::ErrorData> for PostgresError {
    fn from(error: &'a pg_sys::ErrorData) -> Self {
        let elevel = match error.elevel as u32 {
            pg_sys::DEBUG5 => PgLogLevel::DEBUG5,
            pg_sys::DEBUG4 => PgLogLevel::DEBUG5,
            pg_sys::DEBUG3 => PgLogLevel::DEBUG5,
            pg_sys::DEBUG2 => PgLogLevel::DEBUG5,
            pg_sys::DEBUG1 => PgLogLevel::DEBUG5,
            pg_sys::LOG => PgLogLevel::LOG,
            pg_sys::LOG_SERVER_ONLY => PgLogLevel::LOG_SERVER_ONLY,
            pg_sys::INFO => PgLogLevel::INFO,
            pg_sys::NOTICE => PgLogLevel::NOTICE,
            pg_sys::WARNING => PgLogLevel::WARNING,
            pg_sys::ERROR => PgLogLevel::ERROR,
            pg_sys::FATAL => PgLogLevel::FATAL,
            pg_sys::PANIC => PgLogLevel::PANIC,
            // Unknown error? Instead of panicking, let's assume it's an error
            _ => PgLogLevel::ERROR,
        };
        fn to_str(s: *const std::os::raw::c_char) -> Option<String> {
            if s.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(s) }.to_string_lossy().to_string())
            }
        }
        Self {
            elevel,
            output_to_server: error.output_to_server,
            output_to_client: error.output_to_client,
            #[cfg(not(feature = "pg14"))]
            show_funcname: error.show_funcname,
            hide_stmt: error.hide_stmt,
            hide_ctx: error.hide_ctx,
            filename: to_str(error.filename),
            lineno: error.lineno as usize,
            funcname: to_str(error.funcname),
            domain: to_str(error.domain),
            context_domain: to_str(error.context_domain),
            sqlerrcode: error.sqlerrcode as usize,
            message: to_str(error.message),
            detail: to_str(error.detail),
            detail_log: to_str(error.detail_log),
            hint: to_str(error.hint),
            context: to_str(error.context),
            #[cfg(any(feature = "pg13", feature = "pg14"))]
            backtrace: to_str(error.backtrace),
            message_id: to_str(error.message_id),
            schema_name: to_str(error.schema_name),
            table_name: to_str(error.table_name),
            column_name: to_str(error.column_name),
            datatype_name: to_str(error.datatype_name),
            constraint_name: to_str(error.constraint_name),
            cursorpos: error.cursorpos as usize,
            internalpos: error.internalpos as usize,
            internalquery: to_str(error.internalquery),
            saved_errno: error.saved_errno as usize,
            assoc_context: PgMemoryContexts::For(error.assoc_context),
        }
    }
}
