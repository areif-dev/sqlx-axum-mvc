use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, FromRow, QueryBuilder};

use crate::{BasicType, ColumnValueMap};

#[async_trait]
pub trait SqliteModel {
    type Error: std::error::Error + From<sqlx::Error>;

    fn table_name() -> String;

    fn map_cols_to_vals(&self) -> ColumnValueMap;

    async fn create_table(pool: &sqlx::SqlitePool) -> Result<(), Self::Error>
    where
        Self: Sized;

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
}
