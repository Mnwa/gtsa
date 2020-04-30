use actix::prelude::*;
use scan_fmt::scan_fmt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use crate::gelf::gelf_reader::{GelfData, GelfDataWrapper, GelfLevel};
use reqwest::Client;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub struct SentryProcessorActor {
    dsn: Dsn,
    client: Client,
    prepare_actor: Arc<Addr<PrepareActor>>,
}

impl<'a> SentryProcessorActor {
    pub fn new<T>(secret_link: T, prepare_json_threads: usize) -> Addr<SentryProcessorActor>
    where
        T: Into<Cow<'a, str>>,
    {
        let (protocol, pub_key, host, project) = scan_fmt!(
            &secret_link.into(),
            "{}://{}@{}/{}",
            String,
            String,
            String,
            i32
        )
        .unwrap();

        SentryProcessorActor::create(|_| SentryProcessorActor {
            dsn: Dsn {
                protocol,
                pub_key,
                host,
                project,
            },
            client: Client::new(),
            prepare_actor: Arc::new(PrepareActor::new(prepare_json_threads)),
        })
    }
}

impl Actor for SentryProcessorActor {
    type Context = Context<Self>;
}

impl Handler<GelfProcessorMessage> for SentryProcessorActor {
    type Result = Option<SentryEvent>;

    fn handle(&mut self, msg: GelfProcessorMessage, ctx: &mut Self::Context) -> Self::Result {
        let url = self.dsn.prepare_url();

        let prepare_actor = Arc::clone(&self.prepare_actor);

        let rb = self.client.post(url.as_str());

        ctx.spawn(
            async move {
                let sended_request = prepare_actor.send(msg).await;
                let request = match sended_request {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("mailing prepare request error: {:?}", e);
                        return;
                    }
                };

                match rb.json(&request).send().await {
                    Ok(r) => {
                        println!("sentry response: {}", r.text().await.unwrap());
                        return;
                    }
                    Err(e) => {
                        eprintln!("request sending for sentry error: {:?}", e);
                        return;
                    }
                }
            }
            .into_actor(self),
        );
        None
    }
}

struct Dsn {
    protocol: String,
    pub_key: String,
    host: String,
    project: i32,
}

impl Dsn {
    fn prepare_url(&self) -> String {
        format!(
            "{}://{}/api/{}/store/?sentry_version=5&sentry_key={}&sentry_timestamp={}",
            self.protocol,
            self.host,
            self.project,
            self.pub_key,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )
    }
}

struct PrepareActor;
impl PrepareActor {
    fn new(threads: usize) -> Addr<PrepareActor> {
        SyncArbiter::start(threads, || PrepareActor)
    }
}
impl Actor for PrepareActor {
    type Context = SyncContext<Self>;
}

impl Handler<GelfProcessorMessage> for PrepareActor {
    type Result = Option<SentryEvent>;

    fn handle(
        &mut self,
        GelfProcessorMessage(msg): GelfProcessorMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        Some(SentryEvent::from(msg))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SentryLevels {
    Fatal,
    Error,
    Warning,
    Info,
    Debug,
}

impl From<GelfLevel> for SentryLevels {
    fn from(level: GelfLevel) -> Self {
        match level {
            GelfLevel::Emergency => SentryLevels::Fatal,
            GelfLevel::Alert => SentryLevels::Error,
            GelfLevel::Critical => SentryLevels::Error,
            GelfLevel::Error => SentryLevels::Error,
            GelfLevel::Warning => SentryLevels::Warning,
            GelfLevel::Notice => SentryLevels::Warning,
            GelfLevel::Informational => SentryLevels::Info,
            GelfLevel::Debug => SentryLevels::Debug,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SentryExceptionValueMechanism {
    r#type: String,
    data: Map<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct SentryExceptionValue {
    r#type: String,
    value: Value,
    mechanism: Option<SentryExceptionValueMechanism>,
}

#[derive(Serialize, Deserialize)]
struct SentryException {
    values: Vec<SentryExceptionValue>,
}

#[derive(Serialize, Deserialize)]
pub struct SentryEvent {
    event_id: Uuid,
    server_name: String,
    timestamp: f64,
    level: SentryLevels,
    exception: SentryException,
}

impl From<GelfDataWrapper> for SentryEvent {
    fn from(gd: GelfDataWrapper) -> Self {
        let GelfData {
            host,
            level,
            short_message,
            timestamp,
            meta,
            mechanism_data,
            ..
        } = gd.into_gelf();
        let mut exception = SentryException {
            values: meta
                .into_iter()
                .map(|(k, v)| SentryExceptionValue {
                    r#type: k,
                    value: v,
                    mechanism: None,
                })
                .collect::<Vec<SentryExceptionValue>>(),
        };

        exception.values.push(SentryExceptionValue {
            r#type: String::from("GelfException"),
            value: Value::String(short_message),
            mechanism: Some(SentryExceptionValueMechanism {
                r#type: String::from("generic"),
                data: mechanism_data,
            }),
        });

        SentryEvent {
            event_id: uuid::Uuid::new_v4(),
            server_name: host,
            timestamp,
            level: SentryLevels::from(level),
            exception,
        }
    }
}

#[cfg(test)]
mod unpacker {
    use super::*;

    #[test]
    fn test_conert() {
        let s = SentryEvent::from(
            GelfDataWrapper::from_slice(
                br#"{
                        "version":"1.1",
                        "host":"example.org",
                        "short_message":"A short message",
                        "level":5,
                        "_some_info":"foo",
                        "timestamp":1582213226
                    }"#,
            )
            .unwrap(),
        );

        assert!(matches!(s.level, SentryLevels::Warning));
        assert_eq!(s.server_name, "example.org");
        assert_eq!(
            s.exception.values.last().unwrap().value.as_str().unwrap(),
            "A short message"
        );
        assert_eq!(
            s.exception
                .values
                .last()
                .unwrap()
                .mechanism
                .as_ref()
                .unwrap()
                .r#type,
            "generic"
        );
        assert_eq!(
            s.exception.values.first().unwrap().value.as_str().unwrap(),
            "foo"
        );
        assert_eq!(s.exception.values.first().unwrap().r#type, "some_info");
    }

    #[actix_rt::test]
    async fn test_actor() {
        let sentry_prepare = PrepareActor::new(1);

        let s = sentry_prepare
            .send(GelfProcessorMessage(
                GelfDataWrapper::from_slice(
                    br#"{
                        "version":"1.1",
                        "host":"example.org",
                        "short_message":"A short message",
                        "level":5,
                        "_some_info":"foo",
                        "timestamp":1582213226
                    }"#,
                )
                .unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(s.level, SentryLevels::Warning));
        assert_eq!(s.server_name, "example.org");
        assert_eq!(
            s.exception.values.last().unwrap().value.as_str().unwrap(),
            "A short message"
        );
        assert_eq!(
            s.exception
                .values
                .last()
                .unwrap()
                .mechanism
                .as_ref()
                .unwrap()
                .r#type,
            "generic"
        );
        assert_eq!(
            s.exception.values.first().unwrap().value.as_str().unwrap(),
            "foo"
        );
        assert_eq!(s.exception.values.first().unwrap().r#type, "some_info");
    }
}
