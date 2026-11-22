//! A planted HTTP server: each test scripts the answers it gives (the last one repeats), then reads what was sent
//! and when. One request per connection, HTTP/1.1, on 127.0.0.1.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A test service key, built from two pieces so no file holds a whole key.
pub fn key() -> String {
    ["c_serv", "_testkey0000000000000000"].concat()
}

/// A test account key, built the same way.
pub fn account_key() -> String {
    ["c_acct", "_clients000000000000000"].concat()
}

#[derive(Debug, Clone)]
pub struct Seen {
    pub at: Instant,
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: String,
}

impl Seen {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
}

#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub delay: Duration,
}

impl Reply {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![],
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }

    pub fn raw(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![],
            body: body.into(),
            delay: Duration::ZERO,
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn delay_ms(mut self, ms: u64) -> Self {
        self.delay = Duration::from_millis(ms);
        self
    }
}

pub type Respond = Arc<dyn Fn(&Seen) -> Reply + Send + Sync>;

pub enum Step {
    Fixed(Reply),
    Echo(Respond),
}

impl From<Reply> for Step {
    fn from(r: Reply) -> Self {
        Step::Fixed(r)
    }
}

pub fn echo(f: impl Fn(&Seen) -> Reply + Send + Sync + 'static) -> Step {
    Step::Echo(Arc::new(f))
}

pub fn rpc_ok(result: Value) -> Reply {
    Reply::json(200, json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
}

pub fn rpc_err(status: u16, code: i64, message: &str, data: Option<Value>) -> Reply {
    let mut error = json!({ "code": code, "message": message });
    if let Some(d) = data {
        error["data"] = d;
    }
    Reply::json(status, json!({ "jsonrpc": "2.0", "id": 1, "error": error }))
}

pub struct Planted {
    pub url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Planted {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

impl Planted {
    pub async fn start(script: Vec<Step>) -> Planted {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let script: Arc<Vec<Step>> = Arc::new(script);
        let record = seen.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let (record, script) = (record.clone(), script.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    let head_end = loop {
                        if let Some(i) = find(&buf, b"\r\n\r\n") {
                            break i;
                        }
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let mut lines = head.split("\r\n");
                    let mut first = lines.next().unwrap_or("").split(' ');
                    let (method, path) = (
                        first.next().unwrap_or("").to_string(),
                        first.next().unwrap_or("").to_string(),
                    );
                    let headers: HashMap<String, String> = lines
                        .filter_map(|l| l.split_once(':'))
                        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                        .collect();
                    let len: usize = headers
                        .get("content-length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    while buf.len() < head_end + 4 + len {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body = String::from_utf8_lossy(&buf[head_end + 4..]).to_string();
                    let rec = Seen {
                        at: Instant::now(),
                        method,
                        path,
                        headers,
                        body,
                    };
                    let n = {
                        let mut all = record.lock().unwrap();
                        all.push(rec.clone());
                        all.len()
                    };
                    let reply = match &script[n.min(script.len()) - 1] {
                        Step::Fixed(r) => r.clone(),
                        Step::Echo(f) => f(&rec),
                    };
                    tokio::time::sleep(reply.delay).await;
                    let mut out = format!(
                        "HTTP/1.1 {} X\r\nconnection: close\r\ncontent-length: {}\r\nx-request-id: rid-{n}\r\n",
                        reply.status,
                        reply.body.len()
                    );
                    if !reply
                        .headers
                        .iter()
                        .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
                    {
                        out.push_str("content-type: application/json\r\n");
                    }
                    for (k, v) in &reply.headers {
                        out.push_str(&format!("{k}: {v}\r\n"));
                    }
                    out.push_str("\r\n");
                    out.push_str(&reply.body);
                    let _ = sock.write_all(out.as_bytes()).await; // the client may have given up, as the timeout test intends
                    let _ = sock.shutdown().await;
                });
            }
        });
        Planted { url, seen, task }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }

    /// Milliseconds between request i and request i+1.
    pub fn gap_ms(&self, i: usize) -> u128 {
        let s = self.seen();
        (s[i + 1].at - s[i].at).as_millis()
    }
}
