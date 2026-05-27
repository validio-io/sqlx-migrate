use sha2::Sha256;
use state::TypeMap;
use std::{any::Any, sync::Arc};

use sqlx::Database;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use sha2::Digest;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use sqlx::{Execute, Executor, SqlStr};
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use std::borrow::BorrowMut;

pub struct MigrationContext<Db>
where
    Db: Database,
{
    pub(crate) hash_only: bool,
    pub(crate) hasher: Sha256,
    pub(crate) conn: Db::Connection,
    pub(crate) ext: Arc<TypeMap![Send + Sync]>,
}

impl<Db: std::fmt::Debug> std::fmt::Debug for MigrationContext<Db>
where
    Db: Database,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationContext")
            .field("hash_only", &self.hash_only)
            .field("hasher", &self.hasher)
            .field("ext", &self.ext)
            .finish_non_exhaustive()
    }
}

impl<Db> MigrationContext<Db>
where
    Db: Database,
{
    /// Return an executor that can execute queries.
    ///
    /// Currently this just re-borrows self.
    pub fn tx(&mut self) -> &mut Self {
        self
    }

    /// Get an extension.
    #[must_use]
    pub fn get<T: Any>(&self) -> Option<&T> {
        self.ext.try_get()
    }
}

// Implementing this in a generic way confuses the hell out of rustc,
// so instead this is copy/pasted for all supported backends.
#[cfg(feature = "postgres")]
impl<'c> Executor<'c> for &'c mut MigrationContext<sqlx::Postgres> {
    type Database = sqlx::Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<
            itertools::Either<
                <Self::Database as Database>::QueryResult,
                <Self::Database as Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database> + 'q,
    {
        let arguments = match query.take_arguments() {
            Ok(arguments) => arguments,
            Err(error) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(sqlx::Error::Encode(error))
                }))
            }
        };
        let sql = query.sql();
        self.hasher.update(sql.as_str());

        if self.hash_only {
            return self.conn.borrow_mut().fetch_many("");
        }

        self.conn.borrow_mut().fetch_many((sql, arguments))
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database> + 'q,
    {
        let arguments = match query.take_arguments() {
            Ok(arguments) => arguments,
            Err(error) => return Box::pin(async move { Err(sqlx::Error::Encode(error)) }),
        };
        let sql = query.sql();
        self.hasher.update(sql.as_str());

        if self.hash_only {
            return Box::pin(async move { Ok(None) });
        }

        self.conn.borrow_mut().fetch_optional((sql, arguments))
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<<Self::Database as Database>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        self.hasher.update(sql.as_str());
        self.conn.borrow_mut().prepare_with(sql, parameters)
    }

    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        self.hasher.update(sql.as_str());
        self.conn.borrow_mut().describe(sql)
    }
}

// Implementing this in a generic way confuses the hell out of rustc,
// so instead this is copy/pasted for all supported backends.
#[cfg(feature = "sqlite")]
impl<'c> Executor<'c> for &'c mut MigrationContext<sqlx::Sqlite> {
    type Database = sqlx::Sqlite;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<
            itertools::Either<
                <Self::Database as Database>::QueryResult,
                <Self::Database as Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database> + 'q,
    {
        let arguments = match query.take_arguments() {
            Ok(arguments) => arguments,
            Err(error) => {
                return Box::pin(futures_util::stream::once(async move {
                    Err(sqlx::Error::Encode(error))
                }))
            }
        };
        let sql = query.sql();
        self.hasher.update(sql.as_str());

        if self.hash_only {
            return self.conn.borrow_mut().fetch_many("");
        }

        self.conn.borrow_mut().fetch_many((sql, arguments))
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database> + 'q,
    {
        let arguments = match query.take_arguments() {
            Ok(arguments) => arguments,
            Err(error) => return Box::pin(async move { Err(sqlx::Error::Encode(error)) }),
        };
        let sql = query.sql();
        self.hasher.update(sql.as_str());

        if self.hash_only {
            return Box::pin(async move { Ok(None) });
        }

        self.conn.borrow_mut().fetch_optional((sql, arguments))
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> futures_core::future::BoxFuture<
        'e,
        Result<<Self::Database as Database>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        self.hasher.update(sql.as_str());
        self.conn.borrow_mut().prepare_with(sql, parameters)
    }

    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        self.hasher.update(sql.as_str());
        self.conn.borrow_mut().describe(sql)
    }
}
