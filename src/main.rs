use std::{collections::HashMap, sync::Arc};

use tokio::{
    fs::OpenOptions,
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

    fn apply_command(&mut self, command_string: &str) {
        let mut args = command_string.split_whitespace();

        let Some(cmd) = args.next() else {
            return;
        };

        match cmd.to_uppercase().as_str() {
            "SET" => {
                if let (Some(k), Some(v)) = (args.next(), args.next()) {
                    self.set_value(k.to_string(), v.to_string());
                }
            }
            "DEL" => {
                if let Some(k) = args.next() {
                    self.del_value(k);
                }
            }
            _ => {}
        }
    }
}
#[tokio::main]
async fn main() {
    let mut store = KVStore::new();
    if std::path::Path::new("store.aof").exists() {
        let file = std::fs::read_to_string("store.aof").unwrap();
        for line in file.lines() {
            store.apply_command(line);
        }
    }
    let store = Arc::new(RwLock::new(store));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
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
                                writer.write_all(res.as_bytes()).await.unwrap();
                            } else {
                                writer.write_all("(nil)".as_bytes()).await.unwrap();
                            }
                        } else {
                            writer
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
                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("store.aof")
                                .await
                                .unwrap();
                            let log_entry = format!("SET {} {}\n", k, v);
                            file.write_all(log_entry.as_bytes()).await.unwrap();
                            writer.write_all("OK\n".as_bytes()).await.unwrap();
                        } else {
                            writer
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
                                let mut file = OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open("store.aof")
                                    .await
                                    .unwrap();
                                let log_entry = format!("DEL {}\n", arg);
                                file.write_all(log_entry.as_bytes()).await.unwrap();
                                writer.write_all("1\n".as_bytes()).await.unwrap();
                            } else {
                                writer.write_all("0\n".as_bytes()).await.unwrap();
                            }
                        } else {
                            writer
                                .write_all("Missing args for DEL".as_bytes())
                                .await
                                .unwrap();
                        }
                    }
                    _ => {
                        writer
                            .write_all("Invalid request".as_bytes())
                            .await
                            .unwrap();
                    }
                }
            }
        });
    }
}
