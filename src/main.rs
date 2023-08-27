use std::collections::HashMap;
use std::{convert::TryInto, net::SocketAddr};
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};
use serde::{Serialize,Deserialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

struct Subscriber {
    key: String,
    address: String
}

impl Subscriber {
    fn broadcast(&mut self, mut message: Message){
        //let _ = write_socket(&self.address, message);
        tokio::spawn( async move { write_socket(&self.address, message.content).await } );
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:61717").await.unwrap();

    let TOPIC= String::from("topic");
    let CREATE= String::from("create");
    let QUEUE= String::from("queue");
    let SUBSCRIBE= String::from("subscribe");
    let PUSH= String::from("push");
    let EXIT= String::from("exit");

    let mut queue: Queue = Queue {topics: HashMap::new()};

    println!("Hello, world!");

    loop {
        let domain = std::env::args().nth(1).expect("No domain given");
        let operation = std::env::args().nth(2).expect("No operation given");
        let input = std::env::args().nth(3).unwrap();
        let complement = std::env::args().nth(3).unwrap();

        if domain == EXIT {
            break;
        }

        if domain == TOPIC || operation == CREATE{
            let new_topic: Topic = Topic {
                key: input,
                description: "".to_string(),
                subscribers: Default::default(),
                message_list: Default::default(),
            };

            queue.register_topic(new_topic);
        } else if domain == QUEUE {
            if operation == SUBSCRIBE {
                let mut new_subscriber: Subscriber = Subscriber {
                    key: "".to_string(),
                    address: "".to_string()
                };
                new_subscriber.address = input;
                new_subscriber.key = complement;

                queue.subscribe(new_subscriber.key, &new_subscriber);
            } else { // Push
                let mut new_message: Message;
                new_message.content = input;
            }
        }

        match listener.accept().await {
            Ok((socket, addr)) => {
                tokio::spawn( async move { read_socket(socket, addr).await } );
            },
            Err(e) => {
                eprintln!("couldn't get client: {:?}", e);
                continue
            }
        }

    }

}

async fn read_socket(mut socket : TcpStream, addr : SocketAddr) {
    let len = socket.read_u64().await.unwrap();
    let mut data: Vec<u8> = Vec::with_capacity(len.try_into().unwrap());

    socket.read_to_end(&mut data).await.unwrap();
    let msg_received: Message = bincode::deserialize(&data).unwrap();

    println!("New message from {}. Content: {}", addr, msg_received.content);
}

async fn write_socket(address: &String, mut message: String) {

    let listener = TcpListener::bind(address).await;

    match listener.expect("REASON").accept().await {
        Ok((mut socket, _addr)) => {
            // To see who's connected
            //println!(addr.to_string());

            // 1
            let (reader, mut writer) = socket.split();
            let mut reader = tokio::io::BufReader::new(reader);

            match reader.read_line(&mut message).await {
                Ok(_bytes_size) => {
                    // bytes_size can be used somewhere..
                    match writer.write_all(&message.as_bytes()).await {
                        Ok(()) => (),
                        Err(_) => {
                            // error handling
                        }
                    }
                }
                Err(_) => {
                    // error handling
                }
            }
        }
        Err(_) => {
            // error handling (assuming you want to handle the error here)
        }
    }
}

struct Topic {
    key: String,
    description: String,
    subscribers: HashMap<String, Subscriber>,
    message_list: HashMap<String, String>
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Message {
    content: String
}

impl Topic {
    fn subscribe(&mut self, mut subscriber: Subscriber) {
        let mut new_subscriber = Subscriber {key:subscriber.key, address: subscriber.address};
        self.subscribers.insert(new_subscriber.key, new_subscriber);
    }
}

struct Queue {
    topics: HashMap<String, Topic>,
}

impl Queue {
    fn register_topic(&mut self, mut topic: Topic) {
        let mut new_topic = Topic {key: topic.key, description: topic.description, subscribers: topic.subscribers, message_list: topic.message_list};
       self.topics.insert(new_topic.key, new_topic);
    }

    fn subscribe(&mut self, &mut topic_name: String, subscriber: &Subscriber) {
            self.topics.get_mut(topic_name).subscribers.insert(subscriber);
    }

    fn push(&mut self, &mut topic_name: String, message: &Message) {
        self.topics.get(topic_name).message_list.insert(message);

        // Broadcast to the Subscribers of the topic
        for subscriber in self.topics.get(topic_name){
            //subscriber.broad
        }
    }
}