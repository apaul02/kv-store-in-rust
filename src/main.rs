use std::{
    collections::{BTreeMap, HashMap},
    fs,
    hash::DefaultHasher,
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

use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
struct StoreValue {
    data: String,
    expiration: Option<u128>,
}

struct KVStore {
    data: BTreeMap<String, StoreValue>,
    memtable_size: usize,
    bloom_filter: HashMap<String, BloomFilter>,
}

impl KVStore {
    fn new() -> KVStore {
        KVStore {
            data: BTreeMap::new(),
            memtable_size: 0,
            bloom_filter: HashMap::new(),
        }
    }

    fn set_value(&mut self, key: String, value: String, expiration: Option<u128>) {
        let size = key.len() + value.len();
        self.memtable_size += size;
        if self.memtable_size > 1000 {
            self.flush_memtable();
        }
        let _ = self.data.insert(
            key,
            StoreValue {
                data: value,
                expiration,
            },
        );
    }

    fn get_value(&self, key: &str) -> Option<String> {
        if let Some(val) = self.data.get(key) {
            if val.data == "__TOMBSTONE__" {
                return None;
            }
            if let Some(timestamp) = val.expiration {
                if SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    > timestamp
                {
                    return None;
                }
            }
            return Some(val.data.clone());
        }

        if let Ok(entries) = fs::read_dir(".") {
            let mut sstfiles: Vec<String> = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst")
                })
                .map(|entry| entry.file_name().into_string().unwrap())
                .collect();

            sstfiles.sort();
            sstfiles.reverse();

            for file_path in sstfiles {
                if let Some(filter) = self.bloom_filter.get(&file_path) {
                    if !filter.contains(key) {
                        continue;
                    }
                }
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
                            if let Some((timestamp, v)) = v.split_once(',') {
                                let exp_time = timestamp.parse::<u128>().unwrap_or(0);
                                if exp_time > 0
                                    && SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        > exp_time
                                {
                                    return None;
                                }
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
            .insert(
                key.to_string(),
                StoreValue {
                    data: "__TOMBSTONE__".to_string(),
                    expiration: None,
                },
            )
            .is_some()
    }

    fn apply_command(&mut self, command_string: &str) {
        let mut args = command_string.split_whitespace();

        let Some(cmd) = args.next() else {
            return;
        };

        match cmd.to_uppercase().as_str() {
            "SET" => {
                if let (Some(k), Some(t), Some(v)) = (args.next(), args.next(), args.next()) {
                    let timestamp = t.parse::<u128>().unwrap_or(0);
                    let tx = if timestamp == 0 {
                        None
                    } else {
                        Some(timestamp)
                    };
                    self.set_value(k.to_string(), v.to_string(), tx);
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
            let log = format!("{},{},{}\n", key, value.expiration.unwrap_or(0), value.data);
            file.write_all(log.as_bytes()).unwrap();
        }
        let mut filter = BloomFilter::new(10000);

        for key in self.data.keys() {
            filter.insert(key);
        }
        self.bloom_filter.insert(filename, filter);
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
                .map(|entry| entry.file_name().into_string().unwrap())
                .collect();

            sstfiles.sort();
            let mut temp_map: BTreeMap<String, StoreValue> = BTreeMap::new();

            for file_path in &sstfiles {
                if let Ok(contents) = fs::read_to_string(&file_path) {
                    for line in contents.lines() {
                        if let Some((k, v)) = line.split_once(',') {
                            if let Some((t, d)) = v.split_once(',') {
                                let timestamp = t.parse::<u128>().unwrap();
                                let current_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                if timestamp > 0 && current_time > timestamp {
                                    continue;
                                }

                                let exp_option = if timestamp == 0 {
                                    None
                                } else {
                                    Some(timestamp)
                                };
                                temp_map.insert(
                                    k.to_string(),
                                    StoreValue {
                                        data: d.to_string(),
                                        expiration: exp_option,
                                    },
                                );
                            }
                        }
                    }
                }
            }
            temp_map.retain(|_, v| v.data != "__TOMBSTONE__");
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let filename = format!("sstable_{}.sst", timestamp);
            let mut file = std::fs::File::create(&filename).unwrap();
            for (key, value) in &temp_map {
                let log = format!("{},{},{}\n", key, value.expiration.unwrap_or(0), value.data);
                file.write_all(log.as_bytes()).unwrap();
            }

            self.bloom_filter.clear();
            let mut master_filter = BloomFilter::new(10000);
            for key in temp_map.keys() {
                master_filter.insert(key);
            }

            self.bloom_filter.insert(filename, master_filter);

            for file_path in sstfiles {
                std::fs::remove_file(&file_path).unwrap();
            }
        }
    }
}

struct BloomFilter {
    bit_array: Vec<bool>,
}

impl BloomFilter {
    fn new(size: usize) -> BloomFilter {
        BloomFilter {
            bit_array: vec![false; size],
        }
    }

    fn get_indices(&self, key: &str) -> Vec<usize> {
        let mut indices = Vec::new();
        for i in 0..3 {
            let mut hasher = DefaultHasher::new();
            let hash_key = format!("{}{}", key, i);
            hash_key.hash(&mut hasher);

            let index = (hasher.finish() as usize) % self.bit_array.len();
            indices.push(index);
        }
        indices
    }

    fn insert(&mut self, key: &str) {
        let indices = self.get_indices(key);
        for index in indices {
            self.bit_array[index] = true;
        }
    }

    fn contains(&self, key: &str) -> bool {
        let indices = self.get_indices(key);
        for index in indices {
            if !self.bit_array[index] {
                return false;
            }
        }
        true
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
    if let Ok(entries) = std::fs::read_dir(".") {
        for entry in entries.flatten() {
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst") {
                let filename = entry.file_name().into_string().unwrap();
                let mut filter = BloomFilter::new(10000);
                if let Ok(contents) = std::fs::read_to_string(&filename) {
                    for line in contents.lines() {
                        if let Some((k, _)) = line.split_once(',') {
                            filter.insert(k);
                        }
                    }
                }
                store.bloom_filter.insert(filename, filter);
            }
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
                let mut line = String::new();
                let bytes_read = buf_reader.read_line(&mut line).await.unwrap();
                if bytes_read == 0 {
                    break;
                }

                let mut args = Vec::new();

                if line.starts_with('*') {
                    let num_args: usize = line.trim()[1..].parse().unwrap_or(0);

                    for _ in 0..num_args {
                        line.clear();
                        buf_reader.read_line(&mut line).await.unwrap();

                        line.clear();
                        buf_reader.read_line(&mut line).await.unwrap();
                        args.push(line.trim().to_string());
                    }
                } else {
                    args = line.split_whitespace().map(|s| s.to_string()).collect();
                }

                if args.is_empty() {
                    continue;
                }

                let cmd = args[0].to_uppercase();

                match cmd.as_str() {
                    "GET" => {
                        if args.len() > 1 {
                            let key = &args[1];
                            let value_to_send = {
                                let locked_store = store_clone.read().await;
                                locked_store.get_value(key)
                            };

                            if let Some(val) = value_to_send {
                                let res = format!("${}\r\n{}\r\n", val.len(), val);
                                writer.write_all(res.as_bytes()).await.unwrap();
                            } else {
                                writer.write_all("$-1\r\n".as_bytes()).await.unwrap();
                            }
                        } else {
                            writer
                                .write_all(
                                    "-ERR wrong number of arguments for 'get' command\r\n"
                                        .as_bytes(),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    "SET" => {
                        if args.len() > 2 {
                            let key = &args[1];
                            let value = &args[2];

                            let expiration: Option<u128> =
                                if args.len() >= 5 && args[3].to_uppercase() == "EX" {
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let parserd_seconds = args[4].parse::<u128>().unwrap_or(0);
                                    let ex = (parserd_seconds * 1000) + now;
                                    Some(ex)
                                } else {
                                    None
                                };

                            {
                                let mut locked_store = store_clone.write().await;
                                locked_store.set_value(
                                    key.to_string(),
                                    value.to_string(),
                                    expiration,
                                )
                            }

                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("store.aof")
                                .await
                                .unwrap();
                            let log_entry =
                                format!("SET {} {} {}\n", key, expiration.unwrap_or(0), value);
                            file.write_all(log_entry.as_bytes()).await.unwrap();

                            writer.write_all("+OK\r\n".as_bytes()).await.unwrap();
                        } else {
                            writer
                                .write_all(
                                    "-ERR wrong number of arguments for 'set' command\r\n"
                                        .as_bytes(),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    "DEL" => {
                        if args.len() > 1 {
                            let key = &args[1];
                            let response = {
                                let mut locked_store = store_clone.write().await;
                                locked_store.del_value(key)
                            };

                            if response {
                                let mut file = OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open("store.aof")
                                    .await
                                    .unwrap();
                                let log_entry = format!("DEL {}\n", key);
                                file.write_all(log_entry.as_bytes()).await.unwrap();

                                writer.write_all(":1\r\n".as_bytes()).await.unwrap();
                            } else {
                                writer.write_all(":0\r\n".as_bytes()).await.unwrap();
                            }
                        } else {
                            writer
                                .write_all(
                                    "-ERR wrong number of arguments for 'del' command\r\n"
                                        .as_bytes(),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    _ => {
                        let err = format!("-ERR unknown command '{}'\r\n", cmd);
                        writer.write_all(err.as_bytes()).await.unwrap();
                    }
                }
            }
        });
    }
}
