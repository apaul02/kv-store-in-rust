use std::{
    collections::HashMap,
    io::{self, Write},
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
    let mut store = KVStore::new();
    loop {
        let mut input = String::new();

        print!("db> ");
        io::stdout().flush().unwrap();

        io::stdin()
            .read_line(&mut input)
            .expect("Faild to read input");

        let input = input.trim();

        if input == "exit" {
            break;
        }

        let mut args = input.split_whitespace();

        let Some(cmd) = args.next() else {
            continue;
        };

        match cmd.to_uppercase().as_str() {
            "GET" => {
                if let Some(key) = args.next() {
                    match store.get_value(key) {
                        Some(val) => println!("\"{}\"", val),
                        None => println!("(nil)"),
                    }
                } else {
                    println!("(error) ERR wrong number of arguments for 'get' command");
                }
            }
            "SET" => {
                let key = args.next();
                let value = args.next();

                if let (Some(k), Some(v)) = (key, value) {
                    store.set_value(k.to_string(), v.to_string());
                    println!("OK");
                } else {
                    println!("(error) ERR wrong number of arguments for 'set' command");
                }
            }
            "DEL" => {
                if let Some(k) = args.next() {
                    if store.del_value(k) {
                        println!("(integer) 1");
                    } else {
                        println!("(integer) 0");
                    }
                } else {
                    println!("(error) ERR wrong number of arguments for DEL");
                }
            }
            _ => {
                println!("Please enter a valid command");
            }
        }
    }
}
