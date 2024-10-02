use async_trait::async_trait;
use axum::http::StatusCode;

use crate::BasicType;

use super::DbModel;

#[async_trait]
pub trait AxumModel {
    /// Fetch the name of the column that stores the primary key for this Model
    fn primary_col() -> String;

    /// Retrieves a record by its primary key and returns it as JSON.
    ///
    /// # Arguments
    ///
    /// * `pool` - A reference to the `sqlx::SqlitePool` for database interaction.
    /// * `primary_key` - The value of the primary key to search for, wrapped in `BasicType`.
    ///
    /// # Returns
    ///
    /// * `Ok(axum::response::Json<Self>)` - If the record is found, it returns the record as JSON.
    ///
    /// # Errors
    ///
    /// If the operation fails, returns a tuple containing an HTTP `StatusCode` and a string message that explains what went wrong.
    async fn get_json(
        pool: &sqlx::SqlitePool,
        primary_key: BasicType,
    ) -> Result<axum::response::Json<Self>, (StatusCode, String)>
    where
        Self: crate::SqliteDbModel
            + Sized
            + Send
            + Unpin
            + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow>,
        (StatusCode, String): From<<Self as DbModel>::Error>,
    {
        Ok(axum::Json(
            Self::select_one(pool, &Self::primary_col(), primary_key).await?,
        ))
    }

    /// Inserts `self` as a new record and returns it as JSON.
    ///
    /// # Arguments
    ///
    /// * `pool` - A reference to the `sqlx::SqlitePool` for database interaction.
    ///
    /// # Returns
    ///
    /// * `Ok(axum::response::Json<Self>)` - Returns the newly inserted record as JSON.
    ///
    /// # Errors
    ///
    /// If the insertion fails, returns a tuple with a `StatusCode` and an error message explaining what went wrong.
    async fn post_json(
        &self,
        pool: &sqlx::SqlitePool,
    ) -> Result<axum::response::Json<Self>, (StatusCode, String)>
    where
        Self: crate::SqliteDbModel
            + Sized
            + Send
            + Unpin
            + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow>,
        (StatusCode, String): From<<Self as DbModel>::Error>,
    {
        Ok(axum::Json(
            self.insert(pool, &[&Self::primary_col()]).await?,
        ))
    }

    /// Updates an existing record or inserts a new one at the specified primary key, returning the result as JSON.
    ///
    /// # Arguments
    ///
    /// * `pool` - A reference to the `sqlx::SqlitePool` for database interaction.
    ///
    /// # Returns
    ///
    /// * `Ok(axum::response::Json<Self>)` - Returns the upserted record as JSON.
    ///
    /// # Errors
    ///
    /// If the operation fails, returns a tuple containing a `StatusCode` and an error message explaining the issue.
    async fn put_json(
        &self,
        pool: &sqlx::SqlitePool,
    ) -> Result<axum::response::Json<Self>, (StatusCode, String)>
    where
        Self: crate::SqliteDbModel
            + Sized
            + Send
            + Unpin
            + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow>,
        (StatusCode, String): From<<Self as DbModel>::Error>,
    {
        Ok(axum::Json(
            self.upsert(pool, &[], &Self::primary_col()).await?,
        ))
    }

    /// Delete records based on a specified column and value, returning the deleted records as JSON.
    ///
    /// # Arguments
    ///
    /// * `pool` - A reference to the `sqlx::SqlitePool` for database interaction.
    /// * `col` - The name of the column to filter by.
    /// * `val` - The value of `col` to filter by, wrapped in `BasicType`.
    ///
    /// # Returns
    ///
    /// * `Ok(axum::response::Json<Vec<Self>>)` - Returns the deleted records as JSON.
    ///
    /// # Errors
    ///
    /// If the operation fails, returns a tuple containing an HTTP `StatusCode` and a detailed error message describing the failure.
    async fn delete_json(
        pool: &sqlx::SqlitePool,
        col: &str,
        val: BasicType,
    ) -> Result<axum::response::Json<Vec<Self>>, (StatusCode, String)>
    where
        Self: crate::SqliteDbModel
            + Sized
            + Send
            + Unpin
            + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow>,
        (StatusCode, String): From<<Self as DbModel>::Error>,
    {
        Ok(axum::Json(Self::delete(pool, col, val).await?))
    }
}

#[cfg(test)]
mod tests {
    use axum::{async_trait, http::StatusCode};
    use sqlx::prelude::FromRow;

    use crate::{BasicType, ColumnValueMap, SqliteDbModel};

    use super::AxumModel;

    #[derive(Debug)]
    struct Error(sqlx::Error);

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self)
        }
    }

    impl std::error::Error for Error {}

    impl From<sqlx::Error> for Error {
        fn from(value: sqlx::Error) -> Self {
            Error(value)
        }
    }

    impl From<Error> for (StatusCode, String) {
        fn from(value: Error) -> Self {
            match value.0 {
                sqlx::Error::RowNotFound => (
                    StatusCode::NOT_FOUND,
                    format!("Expected one row, but found none. {:?}", value.0),
                ),
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Database query failed with error: {:?}", value.0),
                ),
            }
        }
    }

    #[derive(FromRow)]
    struct TestModel {
        pub id: i64,
        pub name: String,
        pub passwd: String,
        pub created_at: i64,
    }

    #[async_trait]
    impl SqliteDbModel for TestModel {
        type Error = Error;

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

    impl AxumModel for TestModel {
        fn primary_col() -> String {
            "id".to_string()
        }
    }

    #[tokio::test]
    async fn test_post() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        let res = test.post_json(&pool).await.unwrap().0;
        assert_eq!(res.id, 1);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

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
    async fn test_get() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        let res = test.post_json(&pool).await.unwrap().0;

        let res1 = TestModel::get_json(&pool, 1.into()).await.unwrap().0;
        assert_eq!(res.id, res1.id);
        assert_eq!(res.name, res1.name);
        assert_eq!(res.passwd, res1.passwd);
        assert_eq!(res.created_at, res1.created_at);
    }

    #[tokio::test]
    async fn test_put() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let mut test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        let res = test.put_json(&pool).await.unwrap().0;
        assert_eq!(res.id, 18);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        let db_res: TestModel = sqlx::query_as("select * from TestModel")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(db_res.id, 18);
        assert_eq!(db_res.name, test.name);
        assert_eq!(db_res.passwd, test.passwd);
        assert_eq!(db_res.created_at, test.created_at);

        test.name = "test".to_string();
        test.passwd = "Password".to_string();
        let res = test.put_json(&pool).await.unwrap().0;
        assert_eq!(res.id, 18);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        let db_res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(db_res.len(), 1);
        let db_res = db_res.get(0).unwrap();
        assert_eq!(db_res.id, 18);
        assert_eq!(db_res.name, test.name);
        assert_eq!(db_res.passwd, test.passwd);
        assert_eq!(db_res.created_at, test.created_at);
    }

    #[tokio::test]
    async fn test_delete() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        TestModel::create_table(&pool).await.unwrap();
        let test = TestModel {
            id: 18,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        test.put_json(&pool).await.unwrap().0;

        let res = TestModel::delete(&pool, &TestModel::primary_col(), test.id.into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        let res = res.get(0).unwrap();
        assert_eq!(res.id, test.id);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        let res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        let test1 = TestModel {
            id: 19,
            name: "Test".to_string(),
            passwd: "password".to_string(),
            created_at: 1,
        };
        test.put_json(&pool).await.unwrap().0;
        test1.put_json(&pool).await.unwrap().0;

        let res = TestModel::delete_json(&pool, &TestModel::primary_col(), test.id.into())
            .await
            .unwrap()
            .0;
        assert_eq!(res.len(), 1);
        let res = res.get(0).unwrap();
        assert_eq!(res.id, test.id);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        let db_res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(db_res.len(), 1);
        let db_res = db_res.get(0).unwrap();
        assert_eq!(db_res.id, test1.id);
        assert_eq!(db_res.name, test1.name);
        assert_eq!(db_res.passwd, test1.passwd);
        assert_eq!(db_res.created_at, test1.created_at);

        test.put_json(&pool).await.unwrap().0;
        let res = TestModel::delete_json(&pool, "name", "Test".into())
            .await
            .unwrap()
            .0;
        assert_eq!(res.len(), 2);
        let (res, res1) = (res.get(0).unwrap(), res.get(1).unwrap());
        assert_eq!(res.id, test.id);
        assert_eq!(res.name, test.name);
        assert_eq!(res.passwd, test.passwd);
        assert_eq!(res.created_at, test.created_at);

        assert_eq!(res1.id, test1.id);
        assert_eq!(res1.name, test1.name);
        assert_eq!(res1.passwd, test1.passwd);
        assert_eq!(res1.created_at, test1.created_at);

        let db_res: Vec<TestModel> = sqlx::query_as("select * from TestModel")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(db_res.len(), 0);
    }
}
