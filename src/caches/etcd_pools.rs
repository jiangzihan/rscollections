use etcd_client::{Client, ConnectOptions, Error};
use bb8::ManageConnection;
use std::sync::Arc;
use tokio::sync::Mutex;


/// ## 使用方式
/// 
/// ```rust
/// let option = ConnectOptions::default()
/// .with_keep_alive_while_idle(true)
/// .with_keep_alive(Duration::from_secs(60), Duration::from_secs(180));
/// 
/// let manager = EtcdConnectionManager::new(
/// vec!["http://127.0.0.1:2379".to_string()],
/// Some(option)
/// );
/// 
/// // 创建一个连接池，最大连接数为 10
/// let pool = Pool::builder()
/// .max_size(10)
/// .build(manager)
/// .await?;
/// ```
#[derive(Clone)]
pub struct EtcdConnectionManager {
    endpoints: Vec<String>,
    options: Option<ConnectOptions>,
}

impl EtcdConnectionManager {
    pub fn new(endpoints:Vec<String>, options:Option<ConnectOptions>) -> Self {
        Self { endpoints, options }
    }
}

#[async_trait::async_trait]
impl ManageConnection for EtcdConnectionManager {
    type Connection = Arc<Mutex<Client>>;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client = if let Some(ref options) = self.options {
            Client::connect(self.endpoints.clone(), Some(options.clone())).await?
        } else {
            Client::connect(self.endpoints.clone(), None).await?
        };
        Ok(Arc::new(Mutex::new(client)))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // 检查连接是否有效
        conn.lock().await.status().await.map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};
    use bb8::Pool;

    use super::*;

    #[tokio::test]
    async fn test_1() -> Result<(), Box<dyn Error>> {
        let option = ConnectOptions::default()
            .with_keep_alive_while_idle(true)
            .with_keep_alive(Duration::from_secs(60), Duration::from_secs(180));

        let manager = EtcdConnectionManager::new(
            vec!["http://127.0.0.1:2379".to_string()],
            Some(option)
        );
    
        // 创建一个连接池，最大连接数为 10
        let pool = Pool::builder()
            .max_size(10)
            .build(manager)
            .await?;
    
        // 并发任务使用连接池
        let handles: Vec<_> = (0..10).map(|i| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let conn = pool.get().await.unwrap();
                let status = conn.lock().await.status().await;
                match status {
                    Ok(resp) => println!("Task1 {}: {:?}", i, resp),
                    Err(e) => eprintln!("Task {}: Error: {:?}", i, e),
                }
            })
        }).collect();
    
        // 等待所有任务完成
        for handle in handles {
            handle.await.unwrap();
        }
    
        Ok(())
    }
}