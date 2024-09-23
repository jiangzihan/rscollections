use redis::{aio::MultiplexedConnection, Client};
use bb8::ManageConnection;

use crate::error_types::CollectionsError;

#[derive(Clone)]
pub struct RedisConnectionManager {
    url: String,
}

impl RedisConnectionManager {
    pub fn new(url:String) -> Self {
        Self { url }
    }
}

#[async_trait::async_trait]
impl ManageConnection for RedisConnectionManager {
    type Connection = MultiplexedConnection;
    type Error = CollectionsError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client = Client::open(self.url.clone())
        .map_err(|e|CollectionsError::new(crate::error_types::ErrorKind::DbConnectExecption, e))?;

        let conn = client.get_multiplexed_tokio_connection().await
        .map_err(|e|CollectionsError::new(crate::error_types::ErrorKind::DbConnectExecption, e))?;
        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await
        .map_err(|e|CollectionsError::new(crate::error_types::ErrorKind::DbConnectExecption, e))?;

        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err(CollectionsError::new(crate::error_types::ErrorKind::DbConnectExecption,"没有Pong返回")),
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}


#[cfg(test)]
mod tests {
    use std::{env, error::Error, time::Duration};
    use bb8::Pool;
    use futures::future::join_all;
    use redis::AsyncCommands;
    use tokio::time::sleep;
    use tracing::debug;

    use crate::tracing::init_tracing;

    use super::*;

    // 并发请求
    #[tokio::test]
    async fn test_1() -> Result<(), Box<dyn Error>> {
        dotenvy::dotenv().unwrap();
        init_tracing();

        // 链接对象
        let url = env::var("REDIS_URL").unwrap();
        let manager = RedisConnectionManager::new(url);

        // 连接池构造器
        let pool = Pool::builder()
            .max_size(10)
            .build(manager)
            .await?;

        let mut tasks = vec![];
        for i in 0..10 {
            let n = 100;
            let po = pool.clone();
            let task = tokio::spawn(async move {
                for x in 0..n {
                    debug!("i: {}, x: {}", i, x);
                    let mut conn = po.get().await.unwrap();
                    let a:i32 = conn.get("mykey").await.unwrap_or_default();
                    debug!("获取: {}", a);
                    conn.set::<&str, i32, String>("mykey", x).await.unwrap();
                    sleep(Duration::from_secs(3)).await;
                }
            });
            tasks.push(task);
        }
        join_all(tasks).await;

        Ok(())
    }
}