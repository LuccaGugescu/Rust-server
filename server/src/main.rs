use std::io::Read;
use std::net::{TcpListener, TcpStream};


fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").
        unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream);
    }
}


fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    // read request header
    stream.read(&mut buffer).unwrap();
    println!("Request: {}", String::from_utf8_lossy(&buffer));
}