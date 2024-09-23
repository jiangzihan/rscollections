use std::{error::Error, future::Future};

use backoff::{future::{retry as backoff_retry, retry_notify}, Error as BackoffError, ExponentialBackoff};
use tokio::time::Duration;


// 使用 backoff 实现重试逻辑
pub async fn retry<F, Fut, T>(
    operation: F, 
    max_interval: u64,   // 毫秒 ms
    max_elapsed_time: u64,  // 秒 s
    notify: bool   // 是否打开日志
) -> Result<T, i32> 
where 
    F: FnMut()-> Fut+Send,
    Fut: Future<Output = Result<T, BackoffError<i32>>>
{

    let backoff = ExponentialBackoff {
        initial_interval: Duration::from_secs(1),          // 第一次执行 s
        max_interval: Duration::from_millis(max_interval),              // 最长间隔 s, 重试间隔不会指数避退，最大到达此值开始重试
        max_elapsed_time: Some(Duration::from_secs(max_elapsed_time)),  // 最长消耗时间 s, 当总耗时超过此时间停止, 不设该值会永远重试
        ..ExponentialBackoff::default()
    };

    if notify {
        retry_notify(backoff,operation, |err, duration|{
            println!("重试错误: {:?}. 将在 {:?} 后重试", err, duration);
        }).await
    } else {
        backoff_retry(backoff,operation).await
    }
}



#[cfg(test)]
mod tests {
    use tokio::time::sleep;
    use super::*;

    // 模拟一个异步函数，根据不同的 `code` 返回不同的结果
    async fn my_async_function() -> Result<i32, i32> {
        // 模拟获取的code，0=继续重试, 1=终止重试, 2=成功, 其他=错误
        let code = 0;   // 可以根据实际逻辑改变
        println!("执行函数: {}", code);
        sleep(Duration::from_secs(2)).await;
        Err(0)
    }

    #[tokio::test]
    async fn test1() {

        let operation = || async {
            // 执行函数并，确定哪些是正确结果，哪些是要重试，哪些要停止
            match my_async_function().await {
                Ok(result) => Ok(result),
                Err(e) => {
                    match e {
                        0 => Err(BackoffError::transient(0)),
                        1 => Err(BackoffError::permanent(1)),
                        _ => Err(BackoffError::permanent(1)),
                    }
                }
            }
        };

        match retry(operation, 300, 30, true).await {
            Ok(value) => println!("{value}"),
            Err(e) => {
                match e {
                    1 => println!("重试终止"),
                    _ => println!("重试失败，到达最大重试次数")
                }
            },
        };
    
    }

    
}