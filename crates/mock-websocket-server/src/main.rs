/* Adapted from: https://github.com/denoland/fastwebsockets/blob/main/examples/echo_server.rs */
use fastwebsockets::upgrade;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use fastwebsockets::Payload;
use fastwebsockets::WebSocketError;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use tokio::net::TcpListener;

async fn handle_client(fut: upgrade::UpgradeFut) -> Result<(), WebSocketError> {
  let mut ws = fastwebsockets::FragmentCollector::new(fut.await?);

  for i in 1..10 {
    ws.write_frame(Frame::text(Payload::Owned(format!("Hello, Fluvio! - {}", i).bytes().collect()))).await?;
  }

  loop {
    let frame = ws.read_frame().await?;
    match frame.opcode {
      OpCode::Close => break,
      OpCode::Text | OpCode::Binary => {
        ws.write_frame(frame).await?;
      }
      _ => {}
    }
  }

  Ok(())
}
async fn server_upgrade(
  mut req: Request<Incoming>,
) -> Result<Response<Empty<Bytes>>, WebSocketError> {
  let (response, fut) = upgrade::upgrade(&mut req)?;

  tokio::task::spawn(async move {
    if let Err(e) = tokio::task::unconstrained(handle_client(fut)).await {
      eprintln!("Error in websocket connection: {}", e);
    }
  });

  Ok(response)
}

fn main() -> Result<(), WebSocketError> {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_io()
    .build()
    .unwrap();

  rt.block_on(async move {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server started, listening on {}", "127.0.0.1:8080");
    loop {
      let (stream, _) = listener.accept().await?;
      println!("Client connected");
      tokio::spawn(async move {
        let io: hyper_util::rt::TokioIo<tokio::net::TcpStream> = hyper_util::rt::TokioIo::new(stream);
        let conn_fut = http1::Builder::new()
          .serve_connection(io, service_fn(server_upgrade))
          .with_upgrades();
        if let Err(e) = conn_fut.await {
          println!("An error occurred: {:?}", e);
        }
      });
    }
  })
}