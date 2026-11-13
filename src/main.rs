#![forbid(unsafe_code)]

mod app_error;
mod app_server;
mod app_state;
mod auth;
mod avatar;
mod avatar_hash;
mod debug;
mod handlers;
mod narrow;
mod notice;
mod notification_data;
mod notification_sender;
mod queues;
mod rabbitmq;
mod response;
mod secrets;
mod shutdown;
mod types;
mod upload;

use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::app_server::AppServer;
use crate::app_state::AppState;
use crate::avatar::AvatarSettings;
use crate::notification_sender::{NotificationSender, run_publisher};
use crate::queues::Queues;
use crate::rabbitmq::RabbitMQ;
use crate::secrets::Secrets;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    address: SocketAddr,
    #[arg(long)]
    secrets_file: String,
    #[arg(long)]
    rabbitmq_host: String,
    #[arg(long)]
    rabbitmq_user: String,
    #[arg(long)]
    rabbitmq_notify_queue: String,
    #[arg(long)]
    enable_gravatar: bool,
    #[arg(long)]
    default_avatar_uri: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    // TODO/boq: console_subscriber is only suitable in development, not production.
    console_subscriber::init();

    let (shutdown_tx, shutdown_rx) =
        shutdown::channel().with_context(|| "failed to initialize shutdown handler")?;

    let Secrets {
        local_database_password,
        rabbitmq_password,
        secret_key,
        shared_secret,
        avatar_salt,
    } = Secrets::load(&args.secrets_file).with_context(|| "failed to load secrets")?;

    // TODO/boq: support non-development database configuration
    let mut db_config = deadpool_postgres::Config::new();
    db_config.host = Some("localhost".to_string());
    db_config.user = Some("zulip".to_string());
    db_config.password = Some(local_database_password);
    db_config.dbname = Some("zulip".to_string());
    let db_pool = db_config.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )?;

    let rabbitmq = RabbitMQ::connect(
        args.rabbitmq_host,
        args.rabbitmq_user,
        rabbitmq_password,
        &args.rabbitmq_notify_queue,
    )
    .await
    .with_context(|| "failed to connect to RabbitMQ")?;

    let (notification_sender, rx) = NotificationSender::new();
    let publisher_channel = rabbitmq.channel.clone();
    // Not joined with the rabbitmq and server tasks below. The publisher only
    // returns once every NotificationSender is dropped, which doesn't happen
    // before exit.
    //
    // A publish failure means the RabbitMQ channel is gone, so we shut down.
    tokio::spawn(shutdown_tx.on_error(async move {
        run_publisher(publisher_channel, rx)
            .await
            .inspect_err(|error| tracing::error!("notification publisher failed: {error:#}"))
    }));

    let state = Arc::new(AppState {
        shared_secret,
        secret_key,
        avatar_settings: AvatarSettings {
            enable_gravatar: args.enable_gravatar,
            default_avatar_uri: args.default_avatar_uri,
            avatar_salt,
        },
        shutdown_rx,
        db_pool,
        queues: Mutex::new(Queues::new()),
        notification_sender,
    });

    let server = AppServer::new(&args.address, Arc::clone(&state))
        .await
        .with_context(|| "failed to start server")?;

    let (rabbitmq_result, server_result) = tokio::join!(
        tokio::spawn(shutdown_tx.on_error(rabbitmq.run(state))),
        tokio::spawn(shutdown_tx.on_error(server.run()))
    );
    rabbitmq_result
        .with_context(|| "RabbitMQ failed")?
        .with_context(|| "RabbitMQ failed")?;
    server_result
        .with_context(|| "server failed")?
        .with_context(|| "server failed")?;

    tracing::info!("exited");
    Ok(())
}
