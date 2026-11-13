use amq_protocol_types::{FieldTable, ShortString};
use amq_protocol_uri::{AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo};
use anyhow::{Context, Result};
use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions};
use lapin::{Channel, Connection, ConnectionProperties, Consumer};
use std::sync::Arc;

use crate::app_state::AppState;
use crate::notice::process_notice;

pub struct RabbitMQ {
    pub channel: Channel,
    consumer: Consumer,
}

impl RabbitMQ {
    pub async fn connect(
        host: String,
        username: String,
        password: String,
        notify_queue: &str,
    ) -> Result<RabbitMQ> {
        let port = 5672;
        tracing::debug!("connecting to queue {notify_queue} on {username}@{host}:{port}");

        let notify_queue = ShortString::try_new(notify_queue)
            .with_context(|| "invalid RabbitMQ notify queue name")?;

        let connection = Connection::connect_uri(
            AMQPUri {
                scheme: AMQPScheme::AMQP,
                authority: AMQPAuthority {
                    userinfo: AMQPUserInfo { username, password },
                    host,
                    port,
                },
                vhost: "/".into(),
                query: AMQPQueryString::default(),
            },
            ConnectionProperties::default(),
        )
        .await?;

        let channel = connection.create_channel().await?;
        channel.basic_qos(100, BasicQosOptions::default()).await?;

        let declare = {
            let channel = channel.clone();
            move |queue: ShortString| {
                let channel = channel.clone();
                async move {
                    channel
                        .queue_declare(
                            queue,
                            QueueDeclareOptions {
                                durable: true,
                                ..QueueDeclareOptions::default()
                            },
                            FieldTable::default(),
                        )
                        .await
                }
            }
        };
        let (notify_result, mobile_result, emails_result) = tokio::join!(
            declare(notify_queue.clone()),
            declare("missedmessage_mobile_notifications".into()),
            declare("missedmessage_emails".into()),
        );
        notify_result?;
        mobile_result?;
        emails_result?;

        let notify_consumer = channel
            .basic_consume(
                notify_queue.clone(),
                "consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        tracing::debug!("listening on queue {notify_queue}");

        Ok(RabbitMQ {
            channel,
            consumer: notify_consumer,
        })
    }

    async fn handle_delivery(&mut self, state: &Arc<AppState>, delivery: Delivery) -> Result<()> {
        tracing::debug!(
            "RabbitMQ: delivery_tag={:?} exchange={:?} routing_key={:?} redelivered={:?} data={:?}",
            delivery.delivery_tag,
            delivery.exchange,
            delivery.routing_key,
            delivery.redelivered,
            String::from_utf8_lossy(&delivery.data),
        );
        if let Err(error) = process_delivery(state, &delivery) {
            // On error, we log and ACK to discard the notice. The only errors
            // process_delivery can return are serde failures to parse the
            // notice, so discarding is the correct approach: there is nothing
            // we can do with a malformed notice.
            //
            // If process_delivery ever gains errors worth retrying, we will
            // need to add a retry mechanism here.
            tracing::error!(
                "failed to process delivery <{:?}>: {:#} | payload: {}",
                delivery.delivery_tag,
                error,
                String::from_utf8_lossy(&delivery.data),
            );
        }
        self.channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await?;
        Ok(())
    }

    pub async fn run(mut self, state: Arc<AppState>) -> Result<()> {
        let mut shutdown_rx = state.shutdown_rx.clone();
        loop {
            tokio::select! {
                delivery = self.consumer.next() => {
                    let Some(delivery) = delivery else {
                        anyhow::bail!("RabbitMQ consumer was canceled");
                    };
                    self.handle_delivery(&state, delivery?).await?;
                }
                () = shutdown_rx.wait() => break,
            }
        }
        Ok(())
    }
}

fn process_delivery(state: &Arc<AppState>, delivery: &Delivery) -> Result<()> {
    let notice =
        serde_json::from_slice(&delivery.data).with_context(|| "failed to parse notice")?;
    process_notice(state, notice).with_context(|| "failed to process notice")
}
