use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    spawn,
    sync::RwLock,
};

struct KVStore {
    data: HashMap<String, String>,
}

impl KVStore {
    fn new() -> KVStore {
        KVStore {
            data: HashMap::new(),
        }
    }

    fn set_value(&mut self, key: String, value: String) {
        let _ = self.data.insert(key, value);
    }

    fn get_value(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    fn del_value(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }
}
#[tokio::main]
async fn main() {
    let store = Arc::new(RwLock::new(KVStore::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let store_clone = Arc::clone(&store);
        spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut buf_reader = BufReader::new(reader);

            loop {
                let mut input = String::new();

                let bytes_read = buf_reader.read_line(&mut input).await.unwrap();

                if bytes_read == 0 {
                    break;
                }

                let mut args = input.split_whitespace();

                let Some(cmd) = args.next() else {
                    continue;
                };

                match cmd.to_uppercase().as_str() {
                    "GET" => {
                        if let Some(arg) = args.next() {
                            let value_to_send = {
                                let locked_store = store_clone.read().await;

                                locked_store.get_value(arg).cloned()
                            };
                            if let Some(val) = value_to_send {
                                let res = format!("{}\n", val);
                                let _ = writer.write_all(res.as_bytes()).await.unwrap();
                            } else {
                                let _ = writer.write_all("(nil)".as_bytes()).await.unwrap();
                            }
                        } else {
                            let _ = writer
                                .write_all("Missing args for GET".as_bytes())
                                .await
                                .unwrap();
                        }
                    }
                    "SET" => {
                        let key = args.next();
                        let value = args.next();

                        if let (Some(k), Some(v)) = (key, value) {
                            {
                                let mut locked_store = store_clone.write().await;
                                locked_store.set_value(k.to_string(), v.to_string());
                            }
                            let _ = writer.write_all("OK\n".as_bytes()).await.unwrap();
                        } else {
                            let _ = writer
                                .write_all("Missing args for SET".as_bytes())
                                .await
                                .unwrap();
                        }
                    }
                    "DEL" => {
                        if let Some(arg) = args.next() {
                            let response = {
                                let mut locked_store = store_clone.write().await;
                                locked_store.del_value(arg)
                            };
                            if response {
                                let _ = writer.write_all("1\n".as_bytes()).await.unwrap();
                            } else {
                                let _ = writer.write_all("0\n".as_bytes()).await.unwrap();
                            }
                        } else {
                            let _ = writer
                                .write_all("Missing args for DEL".as_bytes())
                                .await
                                .unwrap();
                        }
                    }
                    _ => {
                        let _ = writer
                            .write_all("Invalid request".as_bytes())
                            .await
                            .unwrap();
                    }
                }
            }
        });
    }
}
