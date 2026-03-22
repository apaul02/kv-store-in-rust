use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    fs::OpenOptions,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    spawn,
    sync::RwLock,
};

struct KVStore {
    data: BTreeMap<String, String>,
    memtable_size: usize,
}

impl KVStore {
    fn new() -> KVStore {
        KVStore {
            data: BTreeMap::new(),
            memtable_size: 0,
        }
    }

    fn set_value(&mut self, key: String, value: String) {
        let size = key.len() + value.len();
        self.memtable_size += size;
        if self.memtable_size > 1000 {
            self.flush_memtable();
        }
        let _ = self.data.insert(key, value);
    }

    fn get_value(&self, key: &str) -> Option<String> {
        if let Some(val) = self.data.get(key) {
            if val == "__TOMBSTONE__" {
                return None;
            }
            return Some(val.clone());
        }

        if let Ok(entries) = fs::read_dir(".") {
            let mut sstfiles: Vec<String> = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst")
                })
                .map(|entry| entry.path().to_str().unwrap().to_string())
                .collect();

            sstfiles.sort();
            sstfiles.reverse();

            for file_path in sstfiles {
                if let Ok(contents) = fs::read_to_string(&file_path) {
                    let lines: Vec<&str> = contents.lines().collect();
                    let search_result = lines.binary_search_by(|line| {
                        if let Some((file_key, _file_name)) = line.split_once(',') {
                            file_key.cmp(key)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    });

                    if let Ok(index) = search_result {
                        let found_line = lines[index];
                        if let Some((_, v)) = found_line.split_once(',') {
                            if v == "__TOMBSTONE__" {
                                return None;
                            } else {
                                return Some(v.to_string());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn del_value(&mut self, key: &str) -> bool {
        let size = key.len() + "__TOMBSTONE__".len();
        self.memtable_size += size;
        if self.memtable_size > 1000 {
            self.flush_memtable();
        }
        self.data
            .insert(key.to_string(), "__TOMBSTONE__".to_string())
            .is_some()
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
    fn flush_memtable(&mut self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("sstable_{}.sst", timestamp);
        let mut file = std::fs::File::create(&filename).unwrap();
        for (key, value) in &self.data {
            let log = format!("{},{}\n", key, value);
            file.write_all(log.as_bytes()).unwrap();
        }
        self.data.clear();
        self.memtable_size = 0;
        std::fs::File::create("store.aof").unwrap();

        if let Ok(entries) = fs::read_dir(".") {
            let sst_count = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst")
                })
                .count();
            if sst_count > 5 {
                println!("Too many SSTables ({}). triggering compaction!", sst_count);
                self.compact_sstables();
            }
        }
    }

    fn compact_sstables(&mut self) {
        if let Ok(entries) = fs::read_dir(".") {
            let mut sstfiles: Vec<String> = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst")
                })
                .map(|entry| entry.path().to_str().unwrap().to_string())
                .collect();

            sstfiles.sort();
            let mut temp_map = BTreeMap::new();

            for file_path in &sstfiles {
                if let Ok(contents) = fs::read_to_string(&file_path) {
                    for line in contents.lines() {
                        if let Some((k, v)) = line.split_once(',') {
                            temp_map.insert(k.to_string(), v.to_string());
                        }
                    }
                }
            }
            temp_map.retain(|_, v| v != "__TOMBSTONE__");
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let filename = format!("sstable_{}.sst", timestamp);
            let mut file = std::fs::File::create(&filename).unwrap();
            for (key, value) in &temp_map {
                let log = format!("{},{}\n", key, value);
                file.write_all(log.as_bytes()).unwrap();
            }

            for file_path in sstfiles {
                std::fs::remove_file(&file_path).unwrap();
            }
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

                                locked_store.get_value(arg)
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
