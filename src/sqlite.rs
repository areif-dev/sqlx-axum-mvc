use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, FromRow};

use crate::{BasicType, ColumnValueMap};

#[async_trait]
pub trait SqliteModel {
    /// Custom error type for the model, which must implement the standard `Error` trait and be convertible from `sqlx::Error`
    type Error: std::error::Error + From<sqlx::Error>;

    /// Returns the table name associated with the model.
    ///
    /// # Returns
    /// A `String` representing the table name.
    fn table_name() -> String;

    /// Maps the model's columns to their corresponding values.
    ///
    /// # Returns
    /// - A `ColumnValueMap` where each key is a column name and each value is a `BasicType`.
    fn map_cols_to_vals(&self) -> ColumnValueMap;

    /// Creates the corresponding table for the model in the database.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    ///
    /// # Returns
    /// - `Result<(), Self::Error>`: Returns `Ok` if the table is created successfully, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails.
    async fn create_table(pool: &sqlx::SqlitePool) -> Result<(), Self::Error>
    where
        Self: Sized;

    /// Inserts a new record into the table and returns the newly created model instance.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    /// - `skip_cols`: A list of column names to skip during the insertion. This can be useful for
    /// skipping columns that you would like to be set to their default value by the database. Eg
    /// automatically setting and incrementing the primary key.
    ///
    /// # Returns
    /// - `Result<Self, Self::Error>`: Returns the newly inserted model instance on success, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails.
    async fn insert(&self, pool: &sqlx::SqlitePool, skip_cols: &[&str]) -> Result<Self, Self::Error>
    where
        Self: Sized + for<'r> FromRow<'r, SqliteRow> + Unpin + Send,
    {
        let mut column_names = Vec::new();
        let mut ordered_vals = Vec::new();
        let mut qmarks = Vec::new();
        for (col, val) in self.map_cols_to_vals() {
            if !skip_cols.contains(&col.as_str()) {
                column_names.push(col.to_string());
                ordered_vals.push(val);
                qmarks.push("?");
            }
        }
        let query_str = format!(
            "insert into {} ({}) values ({}) returning *;",
            Self::table_name(),
            column_names.join(","),
            qmarks.join(","),
        );

        let mut query = sqlx::query_as::<sqlx::Sqlite, Self>(&query_str);
        for val in &ordered_vals {
            match val {
                BasicType::Null => {
                    query = query.bind(Option::<String>::None);
                }
                BasicType::Integer(i) => {
                    query = query.bind(i);
                }
                BasicType::Real(f) => {
                    query = query.bind(f);
                }
                BasicType::Text(s) => {
                    query = query.bind(s);
                }
                BasicType::Blob(b) => {
                    query = query.bind(b);
                }
            }
        }
        Ok(query.fetch_one(pool).await?)
    }

    /// Inserts or updates a record in the table depending on whether a conflict occurs on a specific column.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    /// - `skip_cols`: A list of column names to skip during the insertion. This can be useful for
    /// skipping columns that you would like to be set to their default value by the database. Eg
    /// automatically setting and incrementing the primary key.
    /// - `conflict_col`: The name of the column to check for conflicts (usually the primary key).
    ///
    /// # Returns
    /// - `Result<Self, Self::Error>`: Returns the upserted model instance on success, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails.
    async fn upsert(
        &self,
        pool: &sqlx::SqlitePool,
        skip_cols: &[&str],
        conflict_col: &str,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized + for<'r> FromRow<'r, SqliteRow> + Unpin + Send,
    {
        let mut column_names = Vec::new();
        let mut ordered_vals = Vec::new();
        let mut qmarks = Vec::new();
        let mut update_clause = Vec::new();
        for (col, val) in self.map_cols_to_vals() {
            if !skip_cols.contains(&col.as_str()) {
                column_names.push(col.to_string());
                ordered_vals.push(val);
                qmarks.push("?");
                update_clause.push(format!("{} = ?", col));
            }
        }
        let query_str = format!(
            "insert into {} ({}) values ({}) on conflict({}) do update set {} returning *;",
            Self::table_name(),
            column_names.join(","),
            qmarks.join(","),
            conflict_col,
            update_clause.join(","),
        );

        let mut query = sqlx::query_as::<sqlx::Sqlite, Self>(&query_str);
        for _ in 0..2 {
            for val in &ordered_vals {
                match val {
                    BasicType::Null => {
                        query = query.bind(Option::<String>::None);
                    }
                    BasicType::Integer(i) => {
                        query = query.bind(i);
                    }
                    BasicType::Real(f) => {
                        query = query.bind(f);
                    }
                    BasicType::Text(s) => {
                        query = query.bind(s);
                    }
                    BasicType::Blob(b) => {
                        query = query.bind(b);
                    }
                }
            }
        }
        Ok(query.fetch_one(pool).await?)
    }

    /// Selects a single record from the table based on the specified column and value.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    /// - `col`: The name of the column to filter by.
    /// - `val`: The value to filter by, wrapped in `BasicType`.
    ///
    /// # Returns
    /// - `Result<Self, Self::Error>`: Returns the selected model instance on success, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails or if no record matches the filter
    /// or some other `sqlx::Error` occurs.
    async fn select_one(
        pool: &sqlx::SqlitePool,
        col: &str,
        val: BasicType,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized + for<'r> FromRow<'r, SqliteRow> + Unpin + Send,
    {
        let query_str = format!("select * from {} where {} = ?", Self::table_name(), col);
        Ok(match val {
            BasicType::Null => {
                sqlx::query_as(&query_str)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Integer(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Real(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Text(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Blob(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
        })
    }

    /// Selects multiple records from the table based on the specified column and value.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    /// - `col`: The name of the column to filter by.
    /// - `val`: The value to filter by, wrapped in `BasicType`.
    ///
    /// # Returns
    /// - `Result<Vec<Self>, Self::Error>`: Returns a vector of model instances that
    /// match the filter on success, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails.
    async fn select_many(
        pool: &sqlx::SqlitePool,
        col: &str,
        val: BasicType,
    ) -> Result<Vec<Self>, Self::Error>
    where
        Self: Sized + for<'r> FromRow<'r, SqliteRow> + Unpin + Send,
    {
        let query_str = format!("select * from {} where {} = ?", Self::table_name(), col);
        Ok(match val {
            BasicType::Null => {
                sqlx::query_as(&query_str)
                    .bind(Option::<String>::None)
                    .fetch_all(pool)
                    .await?
            }
            BasicType::Integer(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_all(pool)
                    .await?
            }
            BasicType::Real(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_all(pool)
                    .await?
            }
            BasicType::Text(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_all(pool)
                    .await?
            }
            BasicType::Blob(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_all(pool)
                    .await?
            }
        })
    }

    /// Deletes a single record from the table based on the specified column and value and returns the deleted model instance.
    ///
    /// # Arguments
    /// - `pool`: A reference to a `sqlx::SqlitePool` used for database interaction.
    /// - `col`: The name of the column to filter by.
    /// - `val`: The value to filter by, wrapped in `BasicType`.
    ///
    /// # Returns
    /// - `Result<Self, Self::Error>`: Returns the deleted model instance on success, otherwise returns an error.
    ///
    /// # Errors
    /// - Returns `Self::Error` if the database operation fails or if no record matches the filter.
    async fn delete_one(
        pool: &sqlx::SqlitePool,
        col: &str,
        val: BasicType,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized + for<'r> FromRow<'r, SqliteRow> + Unpin + Send,
    {
        let query_str = format!(
            "delete from {} where {} = ? returning *;",
            Self::table_name(),
            col
        );
        Ok(match val {
            BasicType::Null => {
                sqlx::query_as(&query_str)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Integer(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Real(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Text(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
            BasicType::Blob(a) => {
                sqlx::query_as(&query_str)
                    .bind(a)
                    .bind(Option::<String>::None)
                    .fetch_one(pool)
                    .await?
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use sqlx::prelude::FromRow;

    use crate::{BasicType, ColumnValueMap};

    use super::SqliteModel;

    #[derive(FromRow)]

    struct TestModel {
        pub id: i64,
        pub name: String,
        pub passwd: String,
        pub created_at: i64,
    }

    #[async_trait]
    impl SqliteModel for TestModel {
        type Error = sqlx::Error;

        fn table_name() -> String {
            "TestModel".to_string()
        }

        fn map_cols_to_vals(&self) -> ColumnValueMap {
            ColumnValueMap::from([
                ("id".to_string(), BasicType::Integer(self.id)),
                ("name".to_string(), BasicType::Text(self.name.to_string())),
                (
                    "passwd".to_string(),
                    BasicType::Text(self.passwd.to_string()),
                ),
                (
                    "created_at".to_string(),
                    BasicType::Integer(self.created_at),
                ),
            ])
        }

        async fn create_table(pool: &sqlx::SqlitePool) -> Result<(), Self::Error> {
            let query_str = format!(
                r"create table if not exists {} (
                    id integer primary key, 
                    name text not null, 
                    passwd text not null,
                    created_at integer not null default (strftime('%s', 'now'))
                );",
                Self::table_name()
            );

            sqlx::query(&query_str).execute(pool).await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_create_table() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };

        test.insert(&pool, &["id"]).await.unwrap();
        let res: TestModel = sqlx::query_as("select * from TestModel")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(res.id, 1);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);
    }

    #[tokio::test]
    async fn test_upsert() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let mut test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };

        test.upsert(&pool, &["id"], "id").await.unwrap();
        let res: TestModel = sqlx::query_as("select * from TestModel")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(res.id, 1);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        test = TestModel {
            id: 1,
            name: "test".to_string(),
            passwd: "Passwd".to_string(),
            created_at: 2,
        };
        test.upsert(&pool, &[], "id").await.unwrap();
        let res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        let res = res.get(0).unwrap();
        assert_eq!(res.id, 1);
        assert_eq!(res.name, "test".to_string());
        assert_eq!(res.passwd, "Passwd".to_string());
        assert_eq!(res.created_at, 2);

        test = TestModel {
            id: 18,
            name: "another".to_string(),
            passwd: "also".to_string(),
            created_at: 3,
        };
        test.upsert(&pool, &[], "id").await.unwrap();
        let res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        let (first, sec) = (res.get(0).unwrap(), res.get(1).unwrap());
        assert_eq!(first.id, 1);
        assert_eq!(first.name, "test".to_string());
        assert_eq!(first.passwd, "Passwd".to_string());
        assert_eq!(first.created_at, 2);
        assert_eq!(sec.id, 18);
        assert_eq!(sec.name, "another".to_string());
        assert_eq!(sec.passwd, "also".to_string());
        assert_eq!(sec.created_at, 3);
    }

    #[tokio::test]
    async fn test_select_one() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        test.upsert(&pool, &["id"], "id").await.unwrap();

        let res = TestModel::select_one(&pool, "id", BasicType::Integer(1))
            .await
            .unwrap();
        assert_eq!(res.id, 1);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);
    }

    #[tokio::test]
    async fn test_select_many() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        let test1 = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "Password".to_string(),
            created_at: 2,
        };
        test.upsert(&pool, &["id"], "id").await.unwrap();
        test1.upsert(&pool, &["id"], "id").await.unwrap();

        let res = TestModel::select_many(&pool, "id", BasicType::Integer(1))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        let res = TestModel::select_many(&pool, "name", "Test".into())
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        let (res1, res2) = (res.get(0).unwrap(), res.get(1).unwrap());
        assert_eq!(res1.id, 1);
        assert_eq!(res1.name, test.name);
        assert_eq!(res1.passwd, test.passwd);
        assert_eq!(res1.created_at, test.created_at);
        assert_eq!(res2.id, 2);
        assert_eq!(res2.name, test1.name);
        assert_eq!(res2.passwd, test1.passwd);
        assert_eq!(res2.created_at, test1.created_at);
    }

    #[tokio::test]
    async fn test_delete_one() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        let test1 = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "Password".to_string(),
            created_at: 2,
        };
        test.upsert(&pool, &["id"], "id").await.unwrap();
        test1.upsert(&pool, &["id"], "id").await.unwrap();

        let res = TestModel::delete_one(&pool, "id", BasicType::Integer(1))
            .await
            .unwrap();
        assert_eq!(res.id, 1);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        let res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        let res2 = res.get(0).unwrap();
        assert_eq!(res2.id, 2);
        assert_eq!(res2.name, test1.name);
        assert_eq!(res2.passwd, test1.passwd);
        assert_eq!(res2.created_at, test1.created_at);
    }
}
