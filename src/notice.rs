use anyhow::Result;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::app_state::AppState;
use crate::avatar::{AvatarSettings, AvatarSource, get_avatar_field};
use crate::notification_data::{NotificationTrigger, UserIdSets, UserMessageNotificationsData};
use crate::notification_sender::Notification;
use crate::queues::{Client, QueueId, Queues};
use crate::types::{MessageFlags, MessageId, RealmId, UserGroupId, UserId};

#[derive(Debug, Deserialize)]
struct MessageUser {
    id: UserId,
    #[serde(default)]
    flags: MessageFlags,
    #[serde(default)]
    mentioned_user_group_id: Option<UserId>,
}

#[derive(Debug, Deserialize)]
struct LegacyMessageUser {
    stream_push_notify: bool,
    #[serde(default)]
    stream_email_notify: bool,
    #[serde(default)]
    wildcard_mention_notify: bool,
    #[serde(default)]
    sender_is_muted: bool,
    #[serde(default)]
    online_push_enabled: bool,
    #[serde(default)]
    always_push_notify: bool,
    // We can calculate `mentioned` from the usermessage flags, so just remove
    // it
    // #[serde(default)]
    // mentioned: bool,
    #[serde(flatten)]
    user: MessageUser,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MessageUsers {
    Users(Vec<MessageUser>),
    LegacyUsers(Vec<LegacyMessageUser>),
}

#[derive(Clone, Copy, Debug, Deserialize_repr, Eq, PartialEq, Serialize_repr)]
#[repr(u8)]
pub enum EmailAddressVisibility {
    Everyone = 1,
    Members = 2,
    Admins = 3,
    Nobody = 4,
    Moderators = 5,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ContentType {
    #[serde(rename = "text/html")]
    Html,
    #[serde(rename = "text/x-markdown")]
    Markdown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UserDisplayRecipient {
    pub email: String,
    pub full_name: String,
    pub id: UserId,
    pub is_mirror_dummy: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MessageRecipient {
    Private {
        display_recipient: Vec<UserDisplayRecipient>,
    },
    Stream {
        display_recipient: String,
        #[serde(alias = "topic")]
        subject: String,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct WideMessage {
    #[serde(flatten)]
    attrs: HashMap<String, Value>,
    sender_email: String,
    sender_delivery_email: Option<String>,
    sender_id: UserId,
    id: MessageId,
    client: String,
    #[serde(skip)]
    avatar_url: (),
    sender_email_address_visibility: EmailAddressVisibility,
    sender_realm_id: RealmId,
    sender_avatar_source: AvatarSource,
    sender_avatar_version: i32,
    #[serde(skip)]
    content_type: (),
    rendered_content: String,
    content: String,
    #[serde(flatten)]
    recipient: MessageRecipient,
    recipient_type: i16,
    recipient_type_id: i32,
    sender_is_mirror_dummy: bool,
    #[serde(skip)]
    invite_only_stream: (),
}

#[derive(Clone, Debug, Serialize)]
pub struct Message {
    #[serde(flatten)]
    pub attrs: HashMap<String, Value>,
    pub sender_email: String,
    pub sender_id: UserId,
    pub id: MessageId,
    pub client: String,
    pub avatar_url: Option<String>,
    pub content_type: ContentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rendered_content: Option<String>,
    pub content: String,
    #[serde(flatten)]
    pub recipient: MessageRecipient,
    #[serde(skip_serializing_if = "is_false")]
    pub invite_only_stream: bool,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct MessageFlavor {
    apply_markdown: bool,
    client_gravatar: bool,
}

impl WideMessage {
    fn finalize_payload(
        &self,
        &MessageFlavor {
            apply_markdown,
            mut client_gravatar,
        }: &MessageFlavor,
        keep_rendered_content: bool,
        invite_only_stream: bool,
        avatar_settings: &AvatarSettings,
    ) -> Message {
        if self.sender_email_address_visibility != EmailAddressVisibility::Everyone {
            // If email address of the sender is only available to
            // administrators, clients cannot compute gravatars, so we force-set
            // it to false. If we plumbed the current user's role, we could
            // allow client_gravatar=True here if the current user's role has
            // access to the target user's email address.
            client_gravatar = false;
        }
        let avatar_url = get_avatar_field(
            self.sender_id,
            self.sender_realm_id,
            self.sender_delivery_email.as_ref().unwrap(),
            self.sender_avatar_source,
            self.sender_avatar_version,
            false,
            client_gravatar,
            avatar_settings,
        );
        let (content_type, content) = if apply_markdown {
            (ContentType::Html, &self.rendered_content)
        } else {
            (ContentType::Markdown, &self.content)
        };
        let rendered_content = keep_rendered_content.then(|| self.rendered_content.clone());
        Message {
            sender_email: self.sender_email.clone(),
            sender_id: self.sender_id,
            id: self.id,
            client: self.client.clone(),
            avatar_url,
            content_type,
            rendered_content,
            content: content.clone(),
            recipient: self.recipient.clone(),
            invite_only_stream,
            attrs: self.attrs.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MessageEvent {
    sender_queue_id: Option<QueueId>,
    #[serde(default)]
    stream_name: Option<String>,
    #[serde(default)]
    invite_only: bool,
    #[serde(default)]
    realm_id: Option<RealmId>,
    local_id: Option<String>,

    #[serde(default)]
    presence_idle_user_ids: HashSet<UserId>,
    #[serde(default)]
    online_push_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer
    // directly upgrade from 7.x to main.
    #[serde(default, alias = "pm_mention_push_disabled_user_ids")]
    dm_mention_push_disabled_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer
    // directly upgrade from 7.x to main.
    #[serde(default, alias = "pm_mention_email_disabled_user_ids")]
    dm_mention_email_disabled_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_push_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_email_user_ids: HashSet<UserId>,
    #[serde(default)]
    topic_wildcard_mention_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer directly
    // upgrade from 7.x to main.
    #[serde(default, alias = "wildcard_mention_user_ids")]
    stream_wildcard_mention_user_ids: HashSet<UserId>,
    #[serde(default)]
    followed_topic_push_user_ids: HashSet<UserId>,
    #[serde(default)]
    followed_topic_email_user_ids: HashSet<UserId>,
    #[serde(default)]
    topic_wildcard_mention_in_followed_topic_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_wildcard_mention_in_followed_topic_user_ids: HashSet<UserId>,
    #[serde(default)]
    muted_sender_user_ids: HashSet<UserId>,
    #[serde(default)]
    all_bot_user_ids: HashSet<UserId>,
    #[serde(default)]
    disable_external_notifications: bool,

    message_dict: WideMessage,
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageUserInternalData {
    #[serde(flatten)]
    user_notifications_data: UserMessageNotificationsData,
    mentioned_user_group_id: Option<UserGroupId>,
    #[serde(flatten)]
    notified: Notified,
}

fn receiver_is_off_zulip(queues: &Queues, user_id: UserId) -> bool {
    !queues.for_user(user_id).is_some_and(|client_keys| {
        client_keys
            .iter()
            .any(|&client_key| queues.get(client_key).accepts_messages())
    })
}

fn is_false(b: &bool) -> bool {
    !b
}

#[derive(Clone, Debug, Default, Serialize)]
struct Notified {
    #[serde(skip_serializing_if = "is_false")]
    push_notified: bool,
    #[serde(skip_serializing_if = "is_false")]
    email_notified: bool,
}

#[derive(Debug, Serialize)]
pub struct OfflineNotice {
    user_profile_id: UserId,
    message_id: MessageId,
    trigger: NotificationTrigger,
    mentioned_user_group_id: Option<UserGroupId>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum OfflinePushNotice {
    Add(OfflineNotice),

    #[allow(dead_code)]
    Remove {
        user_profile_id: UserId,
        message_ids: Vec<MessageId>,
    },
}

/// See <https://zulip.readthedocs.io/en/latest/subsystems/notifications.html>
/// for high-level design documentation.
fn maybe_enqueue_notifications(
    state: &Arc<AppState>,
    user_notifications_data: &UserMessageNotificationsData,
    acting_user_id: UserId,
    message_id: MessageId,
    mentioned_user_group_id: Option<UserGroupId>,
    idle: bool,
    already_notified: &Notified,
) -> Result<Notified> {
    let mut notified = Notified::default();

    if !already_notified.push_notified
        && let Some(trigger) =
            user_notifications_data.get_push_notification_trigger(acting_user_id, idle)
    {
        let notice = OfflinePushNotice::Add(OfflineNotice {
            user_profile_id: user_notifications_data.user_id,
            message_id,
            trigger,
            mentioned_user_group_id,
        });
        state
            .notification_sender
            .send(Notification::MobilePush(notice));
        notified.push_notified = true;
    }

    // Send missed_message emails if a direct message or a mention. Eventually,
    // we'll add settings to allow email notifications to match the model of
    // push notifications above.
    if !already_notified.email_notified
        && let Some(trigger) =
            user_notifications_data.get_email_notification_trigger(acting_user_id, idle)
    {
        let notice = OfflineNotice {
            user_profile_id: user_notifications_data.user_id,
            message_id,
            trigger,
            mentioned_user_group_id,
        };
        state.notification_sender.send(Notification::Email(notice));
        notified.email_notified = true;
    }

    Ok(notified)
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SpecialClientEvent {
    Message {
        message: Arc<Message>,
        flags: MessageFlags,
        #[serde(skip_serializing_if = "Option::is_none")]
        internal_data: Option<MessageUserInternalData>,
        #[serde(skip_serializing_if = "Option::is_none")]
        local_message_id: Option<String>,
    },
    UpdateMessage {
        #[serde(flatten)]
        attrs: Arc<HashMap<String, Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_name: Option<String>,
        message_id: MessageId,
        // TODO/compatibility: Make this required when one can no longer directly
        // update from 4.x to main.
        #[serde(skip_serializing_if = "Option::is_none")]
        rendering_only: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        user_id: Option<UserId>,
        flags: MessageFlags,
        #[serde(skip_serializing_if = "Option::is_none")]
        mentioned_user_group_id: Option<UserId>,
    },
    DeleteMessage {
        #[serde(flatten)]
        attrs: Arc<HashMap<String, Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_ids: Option<Arc<[MessageId]>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<MessageId>,
    },
    Presence {
        #[serde(flatten)]
        slim_presence: Arc<SlimPresence>,
        #[serde(skip_serializing_if = "Option::is_none")]
        email: Option<String>,
    },
    #[serde(rename = "presence")]
    PresenceModern {
        presences: Arc<HashMap<String, ModernPresence>>,
    },
    CustomProfileFields {
        fields: Arc<[CustomProfileField]>,
    },
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum ClientEvent {
    Special(SpecialClientEvent),
    Other {
        r#type: String,
        #[serde(flatten)]
        attrs: Arc<HashMap<String, Value>>,
    },
}

fn enqueue_message_to_client(
    wide_message: &WideMessage,
    flavor_cache: &mut HashMap<MessageFlavor, Arc<Message>>,
    client: &mut Client,
    flags: &MessageFlags,
    is_sender: bool,
    internal_data: Option<&MessageUserInternalData>,
    invite_only: bool,
    local_id: Option<&String>,
    avatar_settings: &AvatarSettings,
) {
    if !client.accepts_messages() {
        // The actual check is the accepts_event() check below; this line is
        // just an optimization to avoid copying message data unnecessarily
        return;
    }

    let sending_client = &wide_message.client;
    let client_info = client.info();

    // Make sure Zephyr mirroring bots know whether stream is invite-only
    let invite_only_stream = client_info.client_type_name.contains("mirror") && invite_only;

    let flavor = MessageFlavor {
        apply_markdown: client_info.apply_markdown,
        client_gravatar: client_info.client_gravatar,
    };
    let message = Arc::clone(flavor_cache.entry(flavor).or_insert_with_key(|flavor| {
        Arc::new(wide_message.finalize_payload(flavor, false, invite_only_stream, avatar_settings))
    }));

    let user_event = ClientEvent::Special(SpecialClientEvent::Message {
        message,
        flags: flags.clone(),
        internal_data: internal_data.cloned(),
        local_message_id: if is_sender { local_id.cloned() } else { None },
    });

    if !client.accepts_event(&user_event) {
        return;
    }

    // The below prevents (Zephyr) mirroring loops.
    if sending_client.contains("mirror")
        && sending_client.to_lowercase() == client_info.client_type_name.to_lowercase()
    {
        return;
    }

    client.add_event(user_event);
}

/// See
/// <https://zulip.readthedocs.io/en/latest/subsystems/sending-messages.html>
/// for high-level documentation on this subsystem.
fn process_message_event(
    state: &Arc<AppState>,
    mut event_template: MessageEvent,
    users: MessageUsers,
) -> Result<()> {
    let users = match users {
        MessageUsers::Users(users) => users,

        // do_send_messages used to send events with users in dict format, with
        // the dict containing the user_id and other data. We later trimmed down
        // the user data to only contain the user_id and the usermessage flags,
        // and put everything else in the event dict as lists. This block
        // handles any old-format events still in the queue during upgrade.
        //
        // TODO/compatibility: Remove this whole block once one can no longer
        // directly upgrade directly from 4.x to 5.0-dev.
        MessageUsers::LegacyUsers(legacy_users) => legacy_users
            .into_iter()
            .map(|legacy_user| {
                let user_id = legacy_user.user.id;
                if legacy_user.stream_push_notify {
                    event_template.stream_push_user_ids.insert(user_id);
                }
                if legacy_user.stream_email_notify {
                    event_template.stream_email_user_ids.insert(user_id);
                }
                if legacy_user.wildcard_mention_notify {
                    event_template
                        .stream_wildcard_mention_user_ids
                        .insert(user_id);
                }
                if legacy_user.sender_is_muted {
                    event_template.muted_sender_user_ids.insert(user_id);
                }

                // TODO/compatibility: Another translation code block for the
                // rename of `always_push_notify` to `online_push_enabled`.
                // Remove this when one can no longer directly upgrade from 4.x
                // to 5.0-dev.
                if legacy_user.online_push_enabled || legacy_user.always_push_notify {
                    event_template.online_push_user_ids.insert(user_id);
                }

                legacy_user.user
            })
            .collect(),
    };

    tracing::debug!("processing message event {event_template:?} {users:?}");

    let presence_idle_user_ids = event_template.presence_idle_user_ids;

    let mut wide_message = event_template.message_dict;

    // TODO/compatibility: Zulip servers that have message events in their event
    // queues and upgrade to the new version that expects sender_delivery_email
    // in these events will throw errors processing events. We can remove this
    // alias once we don't expect anyone to be directly upgrading from 2.0.x to
    // the latest Zulip.
    wide_message
        .sender_delivery_email
        .get_or_insert_with(|| wide_message.sender_email.clone());

    let sender_id = wide_message.sender_id;
    let message_id = wide_message.id;

    let user_id_sets = UserIdSets {
        private_message: matches!(wide_message.recipient, MessageRecipient::Private { .. }),
        disable_external_notifications: event_template.disable_external_notifications,
        online_push_user_ids: event_template.online_push_user_ids,
        dm_mention_push_disabled_user_ids: event_template.dm_mention_push_disabled_user_ids,
        dm_mention_email_disabled_user_ids: event_template.dm_mention_email_disabled_user_ids,
        stream_push_user_ids: event_template.stream_push_user_ids,
        stream_email_user_ids: event_template.stream_email_user_ids,
        topic_wildcard_mention_user_ids: event_template.topic_wildcard_mention_user_ids,
        stream_wildcard_mention_user_ids: event_template.stream_wildcard_mention_user_ids,
        followed_topic_push_user_ids: event_template.followed_topic_push_user_ids,
        followed_topic_email_user_ids: event_template.followed_topic_email_user_ids,
        topic_wildcard_mention_in_followed_topic_user_ids: event_template
            .topic_wildcard_mention_in_followed_topic_user_ids,
        stream_wildcard_mention_in_followed_topic_user_ids: event_template
            .stream_wildcard_mention_in_followed_topic_user_ids,
        muted_sender_user_ids: event_template.muted_sender_user_ids,
        all_bot_user_ids: event_template.all_bot_user_ids,
    };

    let mut flavor_cache = HashMap::new();
    let mut queues = state.queues.lock().unwrap();

    let processed_user_ids: HashSet<UserId> = users
        .into_iter()
        .map(|user_data| {
            let user_profile_id = user_data.id;
            let flags = &user_data.flags;
            let mentioned_user_group_id = user_data.mentioned_user_group_id;

            // If the recipient was offline and the message was a (1:1 or group)
            // direct message to them or they were @-notified potentially notify
            // more immediately
            let user_notifications_data = UserMessageNotificationsData::from_user_id_sets(
                user_profile_id,
                flags,
                &user_id_sets,
            );

            // If the message isn't notifiable had the user been idle, then the user
            // shouldn't receive notifications even if they were online. In that
            // case we can avoid the more expensive `receiver_is_off_zulip` call,
            // and move on to process the next user.
            let notified = if user_notifications_data.is_notifiable(sender_id, true) {
                let idle = receiver_is_off_zulip(&queues, user_profile_id)
                    || presence_idle_user_ids.contains(&user_profile_id);
                maybe_enqueue_notifications(
                    state,
                    &user_notifications_data,
                    sender_id,
                    message_id,
                    mentioned_user_group_id,
                    idle,
                    &Notified {
                        push_notified: false,
                        email_notified: false,
                    },
                )?
            } else {
                Notified::default()
            };

            let internal_data = MessageUserInternalData {
                user_notifications_data,
                mentioned_user_group_id,
                notified,
            };

            if let Some(client_keys) = queues.for_user(user_profile_id) {
                for client_key in client_keys.clone() {
                    let client = queues.get_mut(client_key);
                    let is_sender = Some(client.queue_id) == event_template.sender_queue_id;
                    enqueue_message_to_client(
                        &wide_message,
                        &mut flavor_cache,
                        client,
                        &user_data.flags,
                        is_sender,
                        Some(&internal_data),
                        event_template.invite_only,
                        event_template.local_id.as_ref(),
                        &state.avatar_settings,
                    );
                }
            }

            Ok(user_profile_id)
        })
        .collect::<Result<_>>()?;

    if event_template.stream_name.is_some()
        && !event_template.invite_only
        && let Some(realm_id) = event_template.realm_id
        && let Some(client_keys) = queues.for_realm_all_streams(realm_id)
    {
        for client_key in client_keys.clone() {
            let client = queues.get_mut(client_key);

            if processed_user_ids.contains(&client.info().user_profile_id) {
                continue;
            }

            let is_sender = Some(client.queue_id) == event_template.sender_queue_id;
            enqueue_message_to_client(
                &wide_message,
                &mut flavor_cache,
                client,
                &MessageFlags::new(),
                is_sender,
                None,
                event_template.invite_only,
                event_template.local_id.as_ref(),
                &state.avatar_settings,
            );
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct UpdateMessageEvent {
    #[serde(flatten)]
    pub attrs: Arc<HashMap<String, Value>>,

    #[serde(default)]
    stream_name: Option<String>,
    message_id: MessageId,
    // TODO/compatibility: Make this required when one can no longer directly
    // update from 4.x to main.
    #[serde(default)]
    rendering_only: Option<bool>,
    #[serde(default)]
    user_id: Option<UserId>,

    #[serde(default)]
    prior_mention_user_ids: HashSet<UserId>,
    #[serde(default)]
    presence_idle_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer directly
    // upgrade from 7.x to main.
    #[serde(default, alias = "pm_mention_push_disabled_user_ids")]
    dm_mention_push_disabled_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer directly
    // upgrade from 7.x to main.
    #[serde(default, alias = "pm_mention_email_disabled_user_ids")]
    dm_mention_email_disabled_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_push_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_email_user_ids: HashSet<UserId>,
    #[serde(default)]
    topic_wildcard_mention_user_ids: HashSet<UserId>,
    // TODO/compatibility: Remove this alias when one can no longer directly
    // upgrade from 7.x to main.
    #[serde(default, alias = "wildcard_mention_user_ids")]
    stream_wildcard_mention_user_ids: HashSet<UserId>,
    #[serde(default)]
    followed_topic_push_user_ids: HashSet<UserId>,
    #[serde(default)]
    followed_topic_email_user_ids: HashSet<UserId>,
    #[serde(default)]
    topic_wildcard_mention_in_followed_topic_user_ids: HashSet<UserId>,
    #[serde(default)]
    stream_wildcard_mention_in_followed_topic_user_ids: HashSet<UserId>,
    #[serde(default)]
    muted_sender_user_ids: HashSet<UserId>,
    #[serde(default)]
    all_bot_user_ids: HashSet<UserId>,
    #[serde(default)]
    disable_external_notifications: bool,
    // TODO/compatibility: Remove this alias when one can no longer directly
    // upgrade from 4.x to main.
    #[serde(default, alias = "push_notify_user_ids")]
    online_push_user_ids: HashSet<UserId>,
}

fn maybe_enqueue_notifications_for_message_update(
    state: &Arc<AppState>,
    queues: &Queues,
    user_notifications_data: &UserMessageNotificationsData,
    message_id: MessageId,
    acting_user_id: UserId,
    private_message: bool,
    presence_idle: bool,
    prior_mentioned: bool,
) -> Result<()> {
    if user_notifications_data.sender_is_muted {
        // Never send notifications if the sender has been muted
        return Ok(());
    }

    if private_message {
        // We don't do offline notifications for direct messages, because we
        // already notified the user of the original message.
        return Ok(());
    }

    if prior_mentioned {
        // Don't spam people with duplicate mentions.  This is especially
        // important considering that most message edits are simple typo
        // corrections.
        //
        // Note that prior_mention_user_ids contains users who received a
        // wildcard mention as well as normal mentions.
        //
        // TODO: Ideally, that would mean that we exclude here cases where
        // user_profile.wildcard_mentions_notify=False and have those still send
        // a notification.  However, we don't have the data to determine whether
        // or not that was the case at the time the original message was sent,
        // so we can't do that without extending the UserMessage data model.
        return Ok(());
    }

    if user_notifications_data.stream_push_notify
        || user_notifications_data.stream_email_notify
        || user_notifications_data.followed_topic_push_notify
        || user_notifications_data.followed_topic_email_notify
    {
        // Currently we assume that if this flag is set to True, then the user
        // already was notified about the earlier message, so we short circuit.
        // We may handle this more rigorously in the future by looking at
        // something like an AlreadyNotified model.
        return Ok(());
    }

    let idle = presence_idle || receiver_is_off_zulip(queues, user_notifications_data.user_id);

    // We don't yet support custom user group mentions for message edit
    // notifications. Users will still receive notifications (because of the
    // mentioned flag), but those will be as if they were mentioned personally.
    let mentioned_user_group_id = None;

    maybe_enqueue_notifications(
        state,
        user_notifications_data,
        acting_user_id,
        message_id,
        mentioned_user_group_id,
        idle,
        &Notified::default(),
    )?;
    Ok(())
}

fn process_update_message_event(
    state: &Arc<AppState>,
    event_template: UpdateMessageEvent,
    users: Vec<MessageUser>,
) -> Result<()> {
    tracing::debug!("processing update_message event {event_template:?} {users:?}");

    let private_message = event_template.stream_name.is_none();
    let user_id_sets = UserIdSets {
        private_message,
        disable_external_notifications: event_template.disable_external_notifications,
        online_push_user_ids: event_template.online_push_user_ids,
        dm_mention_push_disabled_user_ids: event_template.dm_mention_push_disabled_user_ids,
        dm_mention_email_disabled_user_ids: event_template.dm_mention_email_disabled_user_ids,
        stream_push_user_ids: event_template.stream_push_user_ids,
        stream_email_user_ids: event_template.stream_email_user_ids,
        topic_wildcard_mention_user_ids: event_template.topic_wildcard_mention_user_ids,
        stream_wildcard_mention_user_ids: event_template.stream_wildcard_mention_user_ids,
        followed_topic_push_user_ids: event_template.followed_topic_push_user_ids,
        followed_topic_email_user_ids: event_template.followed_topic_email_user_ids,
        topic_wildcard_mention_in_followed_topic_user_ids: event_template
            .topic_wildcard_mention_in_followed_topic_user_ids,
        stream_wildcard_mention_in_followed_topic_user_ids: event_template
            .stream_wildcard_mention_in_followed_topic_user_ids,
        muted_sender_user_ids: event_template.muted_sender_user_ids,
        all_bot_user_ids: event_template.all_bot_user_ids,
    };

    let stream_name = event_template.stream_name;
    let message_id = event_template.message_id;

    // TODO/compatibility: Modern `update_message` events contain the
    // rendering_only key, which indicates whether the update is a link preview
    // rendering update (not a human action). However, because events may be in
    // the notify_tornado queue at the time we upgrade, we need the below logic
    // to compute rendering_only based on the `user_id` key not being present in
    // legacy events that would have had rendering_only set. Remove this check
    // when one can no longer directly update from 4.x to main.
    let rendering_only_update = event_template
        .rendering_only
        .unwrap_or_else(|| event_template.user_id.is_none());

    let mut queues = state.queues.lock().unwrap();

    for user_data in users {
        let user_profile_id = user_data.id;

        // Events where `rendering_only_update` is true come from the
        // do_update_embedded_data code path, and represent rendering previews;
        // there should be no real content changes. Therefore, we know only
        // events where `rendering_only_update` is false possibly send
        // notifications.
        if !rendering_only_update {
            // The user we'll get here will be the sender if the message's
            // content was edited, and the editor for topic edits. That's the
            // correct "acting_user" for both cases.
            let acting_user_id = event_template.user_id.unwrap();

            let flags = &user_data.flags;
            let user_notifications_data = UserMessageNotificationsData::from_user_id_sets(
                user_profile_id,
                flags,
                &user_id_sets,
            );

            maybe_enqueue_notifications_for_message_update(
                state,
                &queues,
                &user_notifications_data,
                message_id,
                acting_user_id,
                private_message,
                event_template
                    .presence_idle_user_ids
                    .contains(&user_profile_id),
                event_template
                    .prior_mention_user_ids
                    .contains(&user_profile_id),
            )?;
        }

        if let Some(client_keys) = queues.for_user(user_profile_id) {
            for client_key in client_keys.clone() {
                let client = queues.get_mut(client_key);
                let user_event = ClientEvent::Special(SpecialClientEvent::UpdateMessage {
                    attrs: Arc::clone(&event_template.attrs),
                    stream_name: stream_name.clone(),
                    message_id,
                    rendering_only: event_template.rendering_only,
                    user_id: event_template.user_id,
                    flags: user_data.flags.clone(),
                    mentioned_user_group_id: user_data.mentioned_user_group_id,
                });
                if client.accepts_event(&user_event) {
                    client.add_event(user_event);
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct DeleteMessageEvent {
    #[serde(flatten)]
    attrs: Arc<HashMap<String, Value>>,
    message_ids: Arc<[MessageId]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDeleteMessageUser {
    id: UserId,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DeleteMessageUsers {
    Ids(Vec<UserId>),
    LegacyUsers(Vec<LegacyDeleteMessageUser>),
}

fn process_delete_message_event(
    state: &AppState,
    event: DeleteMessageEvent,
    users: DeleteMessageUsers,
) {
    let user_ids = match users {
        DeleteMessageUsers::Ids(user_ids) => user_ids,
        DeleteMessageUsers::LegacyUsers(users) => {
            users.into_iter().map(|user_data| user_data.id).collect()
        }
    };

    let user_event = ClientEvent::Special(SpecialClientEvent::DeleteMessage {
        attrs: Arc::clone(&event.attrs),
        message_ids: Some(Arc::clone(&event.message_ids)),
        message_id: None,
    });

    tracing::debug!("processing delete_message event {event:?} {user_ids:?}");

    let mut queues = state.queues.lock().unwrap();

    for user_profile_id in user_ids {
        if let Some(client_keys) = queues.for_user(user_profile_id) {
            for client_key in client_keys.clone() {
                let client = queues.get_mut(client_key);
                if !client.accepts_event(&user_event) {
                    continue;
                }

                // For clients which support message deletion in bulk, we send a
                // list of msgs_ids together, otherwise we send a delete event
                // for each message. All clients will be required to support
                // bulk_message_deletion in the future; this logic is intended
                // for backwards-compatibility only.
                if client.info().bulk_message_deletion {
                    client.add_event(user_event.clone());
                    continue;
                }

                for &message_id in &*event.message_ids {
                    let compatibility_event =
                        ClientEvent::Special(SpecialClientEvent::DeleteMessage {
                            attrs: Arc::clone(&event.attrs),
                            message_ids: None,
                            message_id: Some(message_id),
                        });
                    client.add_event(compatibility_event);
                }
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SlimPresence {
    user_id: UserId,
    server_timestamp: f64,
    presence: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ModernPresence {
    active_timestamp: i64,
    idle_timestamp: i64,
}

#[derive(Debug, Deserialize)]
pub struct PresenceEvent {
    email: String,
    user_id: UserId,
    server_timestamp: f64,
    legacy_presence: HashMap<String, Value>,
    modern_presence: ModernPresence,
}

fn process_presence_event(state: &AppState, event: PresenceEvent, user_ids: Vec<UserId>) {
    tracing::debug!("processing presence event {event:?} {user_ids:?}");

    let PresenceEvent {
        email,
        user_id,
        server_timestamp,
        legacy_presence,
        modern_presence,
    } = event;

    let slim_presence = Arc::new(SlimPresence {
        user_id,
        server_timestamp,
        presence: legacy_presence,
    });

    let slim_event = ClientEvent::Special(SpecialClientEvent::Presence {
        slim_presence: Arc::clone(&slim_presence),
        email: None,
    });
    let legacy_event = ClientEvent::Special(SpecialClientEvent::Presence {
        slim_presence: Arc::clone(&slim_presence),
        email: Some(email),
    });
    let modern_event = ClientEvent::Special(SpecialClientEvent::PresenceModern {
        presences: Arc::new(HashMap::from([(user_id.to_string(), modern_presence)])),
    });

    let mut queues = state.queues.lock().unwrap();

    for user_profile_id in user_ids {
        if let Some(client_keys) = queues.for_user(user_profile_id) {
            for client_key in client_keys.clone() {
                let client = queues.get_mut(client_key);
                if client.accepts_event(&slim_event) {
                    if client.info().simplified_presence_events {
                        client.add_event(modern_event.clone());
                    } else if client.info().slim_presence {
                        client.add_event(slim_event.clone());
                    } else {
                        client.add_event(legacy_event.clone());
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize_repr, Eq, PartialEq, Serialize_repr)]
#[repr(u8)]
pub enum CustomProfileFieldType {
    ShortText = 1,
    LongText = 2,
    Select = 3,
    Date = 4,
    Url = 5,
    User = 6,
    ExternalAccount = 7,
    Pronouns = 8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CustomProfileField {
    id: i32,
    r#type: CustomProfileFieldType,
    order: i32,
    name: String,
    hint: String,
    field_data: String,
    #[serde(default, skip_serializing_if = "is_false")]
    display_in_profile_summary: bool,
}

#[derive(Debug, Deserialize)]
pub struct CustomProfileFieldsEvent {
    fields: Arc<[CustomProfileField]>,
}

fn process_custom_profile_fields_event(
    state: &AppState,
    event: CustomProfileFieldsEvent,
    user_ids: Vec<UserId>,
) {
    tracing::debug!("processing custom_profile_fields event {event:?} {user_ids:?}");

    let pronouns_type_unsupported_fields: Arc<[CustomProfileField]> = event
        .fields
        .iter()
        .map(|field| CustomProfileField {
            r#type: if field.r#type == CustomProfileFieldType::Pronouns {
                CustomProfileFieldType::ShortText
            } else {
                field.r#type
            },
            ..field.clone()
        })
        .collect();

    let user_event = ClientEvent::Special(SpecialClientEvent::CustomProfileFields {
        fields: event.fields,
    });

    let mut queues = state.queues.lock().unwrap();

    for user_profile_id in user_ids {
        if let Some(client_keys) = queues.for_user(user_profile_id) {
            for client_key in client_keys.clone() {
                let client = queues.get_mut(client_key);
                if client.accepts_event(&user_event) {
                    if client.info().pronouns_field_type_supported {
                        client.add_event(user_event.clone());
                    } else {
                        let pronouns_type_unsupported_event =
                            ClientEvent::Special(SpecialClientEvent::CustomProfileFields {
                                fields: Arc::clone(&pronouns_type_unsupported_fields),
                            });
                        client.add_event(pronouns_type_unsupported_event);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CleanupQueueEvent {
    queue_id: QueueId,
}

/// This event may be generated to forward cleanup requests to the right shard.
fn process_cleanup_queue_event(state: &AppState, event: CleanupQueueEvent, (user_id,): (UserId,)) {
    tracing::debug!("processing cleanup_queue event {event:?} {user_id:?}");
    let mut queues = state.queues.lock().unwrap();
    if !queues.delete(user_id, event.queue_id) {
        tracing::info!(
            "Ignoring cleanup request for bad queue id {queue_id} ({user_id})",
            queue_id = event.queue_id,
        );
    }
}

#[derive(Debug, Deserialize)]
pub struct OtherEvent {
    r#type: String,
    #[serde(flatten)]
    attrs: Arc<HashMap<String, Value>>,
}

fn process_other_event(state: &AppState, event: OtherEvent, user_ids: Vec<UserId>) {
    tracing::debug!("processing event {event:?} {user_ids:?}");

    let user_event = ClientEvent::Other {
        r#type: event.r#type,
        attrs: event.attrs,
    };

    let mut queues = state.queues.lock().unwrap();

    for user_profile_id in user_ids {
        if let Some(client_keys) = queues.for_user(user_profile_id) {
            for client_key in client_keys.clone() {
                let client = queues.get_mut(client_key);
                if client.accepts_event(&user_event) {
                    client.add_event(user_event.clone());
                }
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Message(MessageEvent),
    UpdateMessage(UpdateMessageEvent),
    DeleteMessage(DeleteMessageEvent),
    Presence(PresenceEvent),
    CustomProfileFields(CustomProfileFieldsEvent),
    CleanupQueue(CleanupQueueEvent),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub struct Notice<'a> {
    #[serde(borrow)]
    event: &'a RawValue,
    #[serde(borrow)]
    users: &'a RawValue,
}

pub fn process_notice(state: &Arc<AppState>, notice: Notice) -> Result<()> {
    tracing::debug!("processing {notice:?}");

    let Notice { event, users } = notice;

    match serde_json::from_str::<Event>(event.get())? {
        Event::Message(event) => {
            process_message_event(state, event, serde_json::from_str(users.get())?)?;
        }
        Event::UpdateMessage(event) => {
            process_update_message_event(state, event, serde_json::from_str(users.get())?)?;
        }
        Event::DeleteMessage(event) => {
            process_delete_message_event(state, event, serde_json::from_str(users.get())?);
        }
        Event::Presence(event) => {
            process_presence_event(state, event, serde_json::from_str(users.get())?);
        }
        Event::CustomProfileFields(event) => {
            process_custom_profile_fields_event(state, event, serde_json::from_str(users.get())?);
        }
        Event::CleanupQueue(event) => {
            process_cleanup_queue_event(state, event, serde_json::from_str(users.get())?);
        }
        Event::Other => {
            process_other_event(
                state,
                serde_json::from_str(event.get())?,
                serde_json::from_str(users.get())?,
            );
        }
    }

    Ok(())
}
