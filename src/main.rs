use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex, RwLock},
    thread,
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
fn main() {
    let store = Arc::new(RwLock::new(KVStore::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store_clone = Arc::clone(&store);
                thread::spawn(move || {
                    let mut buf_reader = BufReader::new(stream.try_clone().unwrap());
                    loop {
                        let mut input = String::new();

                        let bytes_read = buf_reader.read_line(&mut input).unwrap();
                        if bytes_read == 0 {
                            break;
                        };
                        let mut args = input.trim().split_whitespace();
                        let Some(cmd) = args.next() else {
                            continue;
                        };

                        match cmd.to_uppercase().as_str() {
                            "SET" => {
                                let key = args.next();
                                let value = args.next();

                                if let (Some(k), Some(v)) = (key, value) {
                                    let mut locked_store = store_clone.write().unwrap();
                                    locked_store.set_value(k.to_string(), v.to_string());

                                    let _ = stream.write_all("OK\n".as_bytes());
                                }
                            }
                            "GET" => {
                                if let Some(arg) = args.next() {
                                    let locked_store = store_clone.read().unwrap();
                                    let response = locked_store.get_value(arg);
                                    match response {
                                        Some(val) => {
                                            let res = format!("{}\n", val);
                                            let _ = stream.write_all(res.as_bytes());
                                        }
                                        None => {
                                            let _ = stream.write_all("(nil)".as_bytes());
                                        }
                                    }
                                } else {
                                    let _ = stream
                                        .write_all("Missing args for get request\n".as_bytes());
                                }
                            }
                            "DEL" => {
                                if let Some(arg) = args.next() {
                                    let mut locked_store = store_clone.write().unwrap();
                                    let response = locked_store.del_value(arg);
                                    if response {
                                        let _ = stream.write_all("1\n".as_bytes());
                                    } else {
                                        let _ = stream.write_all("0\n".as_bytes());
                                    }
                                } else {
                                    let _ = stream.write_all("Missing args for DEL".as_bytes());
                                }
                            }
                            _ => {
                                let _ = stream.write_all("Invalid Input\n".as_bytes());
                            }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Erro Occured: {}", e);
            }
        };
    }
}
