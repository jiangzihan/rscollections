// use redis::{AsyncCommands, aio::MultiplexedConnection};
use redis::aio::MultiplexedConnection;
use uuid::Uuid; // 用于生成唯一的锁标识符
use crate::{error_types::CollectionsError, retryable::retry};
use bb8::Pool;
use backoff::Error as BackoffError;
use crate::error_types::ErrorKind;

use super::pool::RedisConnectionManager;

pub struct RedisDistributedLock {
    pool: Pool<RedisConnectionManager>,
}

impl RedisDistributedLock {
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self { pool }
    }

    /// 尝试获取锁, 返回唯一的uuid, 只有自己才能释放自己的锁
    pub async fn acquire_lock(
        &self,
        key: &str,         // 锁名称
        expiration: u64,   // 有效时间,s
    ) -> Result<String, CollectionsError> {

        let lock_value = Uuid::new_v4().to_string(); // 生成唯一的锁标识符

        let operation = || async {
            let _conn = self.pool.get().await.map_err(|e| {
                CollectionsError::new(ErrorKind::DbConnectExecption, e)
            }).map_err(|e|{
                BackoffError::transient(CollectionsError::new(ErrorKind::Unknown, e))
            });

            let mut conn = match _conn {
                Ok(v) => v,
                Err(e) => {
                    return Err(BackoffError::permanent(
                        CollectionsError::new(ErrorKind::DbConnectExecption, e)))
                }
            };

            let result = redis::cmd("SET")
                .arg(key)
                .arg(&lock_value)
                .arg("NX")
                .arg("EX")
                .arg(expiration)
                .query_async::<MultiplexedConnection, Option<String>>(&mut *conn)
                .await
                .map_err(|e| {
                    BackoffError::transient(
                    CollectionsError::new(ErrorKind::Unknown, e))
            });
            match result {
                Ok(Some(_)) => Ok(&lock_value), // 成功获取锁
                Ok(None) => {
                    Err(BackoffError::transient(
                        CollectionsError::new(ErrorKind::DistributedWasLocked, "获取redis分布锁异常,期望重试")))
                },
                _ => {
                    Err(BackoffError::permanent(
                        CollectionsError::new(ErrorKind::DistributedTermLocked, "中止redis分布锁请求")))
                }
            }
        };

        // 最大重试间隔500ms
        match retry(operation, 1, expiration).await {
            Ok(v) => return Ok(v.to_string()), // 成功获取锁
            Err(e) => return Err(e),
        }

    }

    /// 释放锁, 只有获取之前相同的uuid才有权释放锁
    pub async fn release_lock(
        &self,
        key: &str,
        lock_value: &str,
    ) -> Result<(), CollectionsError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            CollectionsError::new(crate::error_types::ErrorKind::DbConnectExecption, e)
        })?;

        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;

        redis::Script::new(script)
        .key(key)
        .arg(lock_value)
        .invoke_async(&mut *conn)
        .await
        .map_err(|e| {
            CollectionsError::new(crate::error_types::ErrorKind::DbOperateExecption, e)
        })?;

        Ok(())
    }
}




#[cfg(test)]
mod tests {
    use std::{env, error::Error, sync::Arc, time::Duration};
    use bb8::Pool;
    use futures::future::join_all;
    use tokio::{join, sync::{mpsc, Semaphore}, time::sleep};
    use tracing::info;
    use std::time::Instant;
    use redis::AsyncCommands;

    use crate::tracing::init_tracing;

    use super::*;

    // 基本请求
    #[tokio::test]
    async fn test_0() -> Result<(), Box<dyn Error>> {
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

        let key = "mykeyname";
        let start = Instant::now();
        let expiration = 1;

        let d = RedisDistributedLock::new(pool.clone());
        let task1 = tokio::spawn(async move {
            let lock_value = d.acquire_lock(key, expiration).await.unwrap();
            // info!("A执行内容, 5s 完成");
            let k = "counter";
            let mut counter = d.pool.get().await.unwrap().get::<&str, u32>(k,).await.unwrap_or_default();
            counter+=1;
            d.pool.get().await.unwrap().set::<&str,u32,String>(k, counter).await.unwrap();
            // info!("A执行完成, 释放锁");
            d.release_lock(key, &lock_value).await.unwrap();
        });

        let _ = join!(task1);
        let duration = start.elapsed();
        info!("最终耗时: {} ms", duration.as_millis());

        Ok(())
    }

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

        let key = "mykeyname";
        let start = Instant::now();

        let expiration = 1;
        let d = RedisDistributedLock::new(pool.clone());
        let task1 = tokio::spawn(async move {
            let lock_value = d.acquire_lock(key, expiration).await.unwrap();
            info!("A执行内容, 5s 完成");
            d.pool.get().await.unwrap().set::<&str, i32, String>("key_a",100).await.unwrap();
            sleep(Duration::from_secs(3)).await;
            info!("A执行完成, 释放锁");
            d.release_lock(key, &lock_value).await.unwrap();
        });

        let d = RedisDistributedLock::new(pool.clone());
        let task2 = tokio::spawn(async move {
            let lock_value = d.acquire_lock(key, expiration).await.unwrap();
            info!("B执行内容, 5s 完成");
            d.pool.get().await.unwrap().set::<&str, i32, String>("key_b",101).await.unwrap();
            sleep(Duration::from_secs(1)).await;
            info!("B执行完成, 释放锁");
            d.release_lock(key, &lock_value).await.unwrap();
        });

        let d = RedisDistributedLock::new(pool.clone());
        let task3 = tokio::spawn(async move {
            let lock_value = d.acquire_lock(key, expiration).await.unwrap();
            info!("B执行内容, 5s 完成");
            d.pool.get().await.unwrap().set::<&str, i32, String>("key_c",102).await.unwrap();
            sleep(Duration::from_secs(1)).await;
            info!("B执行完成, 释放锁");
            d.release_lock(key, &lock_value).await.unwrap();
        });

        join_all(vec![task1,task2,task3]).await;
        let duration = start.elapsed();
        info!("最终耗时: {} s", duration.as_secs());

        Ok(())
    }


    // 大量并发请求数据资源
    #[tokio::test]
    async fn test_2() -> Result<(), Box<dyn Error>> {
        dotenvy::dotenv().unwrap();
        init_tracing();

        // 链接对象
        let url = env::var("REDIS_URL").unwrap();
        let manager = RedisConnectionManager::new(url);

        // 连接池构造器
        let pool = Pool::builder()
            .max_size(300)
            .build(manager)
            .await?;

        let key = "mykeyname";
        let start = Instant::now();
        let expiration = 30;

        let (tx, mut rx) = mpsc::channel::<u32>(10000);
        let semaphore = Arc::new(Semaphore::new(10000));

        // 执行100万次自增1并发操作
        for _ in 0..1_000_000 {
            let d = RedisDistributedLock::new(pool.clone());
            let txc = tx.clone();
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Ok(lock_value) = d.acquire_lock(key, expiration).await {
                    let k = "counter";
                    let mut counter = d.pool.get().await.unwrap().get::<&str, u32>(k,).await.unwrap_or_default();
                    counter+=1;
                    d.pool.get().await.unwrap().set::<&str,u32,String>(k, counter).await.unwrap();
                    txc.send(counter).await.unwrap();
    
                    d.release_lock(key, &lock_value).await.unwrap();
                };

                drop(_permit)
            });
        }

        drop(tx);

        while let Some(_) = rx.recv().await {
            // println!("received: {}", i);
        }

        let duration = start.elapsed();
        info!("最终耗时: {} s", duration.as_secs());
        Ok(())
    }
}