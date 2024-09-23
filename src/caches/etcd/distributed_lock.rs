use std::error::Error;

use etcd_client::LockOptions;
use bb8::Pool;


use super::pool::EtcdConnectionManager;

#[derive(Clone)]
pub struct EtcdDistributedLock {
    pool: Pool<EtcdConnectionManager>,
}

impl EtcdDistributedLock {
    pub fn new(pool: Pool<EtcdConnectionManager>) -> Self {
        Self { pool }
    }

    /// 尝试获取锁
    pub async fn acquire_lock(
        &self,
        key: &str,
        ttl: i64,  // 锁的过期时间
    ) -> Result<Option<String>, Box<dyn Error>> {
        let client = self.pool.get().await?;

        let mut conn = client.lock().await;

        let mut option = None;
        if ttl > 0 {
            let lease = {
                conn.lease_grant(ttl, None).await?
            };
            let lease_id = lease.id();
            option = Some(LockOptions::new().with_lease(lease_id));
        }


        // 使用租约的 ID 绑定键，lock_value 用于区分不同客户端
        let resp = {
            conn.lock(key, option).await?
        };

        let lease_id2 = std::str::from_utf8(resp.key()).unwrap().to_string();
        Ok(Some(lease_id2))
    }

    /// 释放锁
    pub async fn release_lock(
        &self,
        key: &str,
        lock_value: &str,
    ) -> Result<(), Box<dyn Error>> {
        let client = self.pool.get().await?;

        let mut conn = client.lock().await;

        conn.unlock(key).await?;
        Ok(())
    }
}
