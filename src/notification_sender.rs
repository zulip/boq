use anyhow::Context;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::notice::{OfflineNotice, OfflinePushNotice};

pub struct NotificationSender {
    tx: mpsc::Sender<Notification>,
    dropped_count: AtomicU64,
}

#[derive(Debug)]
pub enum Notification {
    MobilePush(OfflinePushNotice),
    Email(OfflineNotice),
}

// Bounds memory if publishes stall for a long time;
// past this, new notifications are dropped rather than queued.
const NOTIFICATION_CHANNEL_CAPACITY: usize = 1_000_000;

impl NotificationSender {
    pub fn new() -> (NotificationSender, mpsc::Receiver<Notification>) {
        let (tx, rx) = mpsc::channel(NOTIFICATION_CHANNEL_CAPACITY);

        let sender = NotificationSender {
            tx,
            dropped_count: AtomicU64::new(0),
        };
        (sender, rx)
    }

    pub fn send(&self, notification: Notification) {
        match self.tx.try_send(notification) {
            Ok(()) => {
                if self.dropped_count.load(Ordering::Relaxed) != 0 {
                    let dropped = self.dropped_count.swap(0, Ordering::Relaxed);
                    if dropped != 0 {
                        tracing::info!(
                            "notification queue has room again; {dropped} notifications were dropped"
                        );
                    }
                }
            }
            Err(TrySendError::Full(notification)) => {
                let previously_dropped = self.dropped_count.fetch_add(1, Ordering::Relaxed);
                if previously_dropped.is_multiple_of(10_000) {
                    tracing::error!(
                        "notification queue is full; dropped {notification:?} ({} dropped so far)",
                        previously_dropped + 1
                    );
                }
            }
            Err(TrySendError::Closed(_)) => {
                tracing::error!("notification publisher channel closed; dropping the notification");
            }
        }
    }
}

pub async fn run_publisher(
    rabbitmq_channel: lapin::Channel,
    mut rx: mpsc::Receiver<Notification>,
) -> anyhow::Result<()> {
    while let Some(notification) = rx.recv().await {
        let (rabbitmq_queue_name, payload) = match &notification {
            Notification::MobilePush(notice) => (
                "missedmessage_mobile_notifications",
                serde_json::to_vec(notice),
            ),
            Notification::Email(notice) => ("missedmessage_emails", serde_json::to_vec(notice)),
        };
        let payload = match payload {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!("failed to serialize notification {notification:?}: {error:#}");
                continue;
            }
        };

        // We .await each publish before starting the next one. If we ever
        // need more throughput here, we can switch to futures-util's
        // join_all to run multiple publishes concurrently, while fetching
        // backlogged notifications in bulk via rx.recv_many().
        rabbitmq_channel
            .basic_publish(
                "".into(),
                rabbitmq_queue_name.into(),
                lapin::options::BasicPublishOptions::default(),
                &payload,
                lapin::BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .with_context(|| {
                format!("failed to publish {notification:?} to {rabbitmq_queue_name}")
            })?;
    }

    Ok(())
}
