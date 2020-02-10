use actix::prelude::*;
use scan_fmt::scan_fmt;
use serde_json::{json, Value};

use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use std::time::{SystemTime, UNIX_EPOCH};
use std::borrow::Borrow;
use reqwest::Client;

pub struct SentryProcessorActor {
    dsn: Dsn,
    client: Client,
}

impl SentryProcessorActor {
    pub fn new(secret_link: &str) -> Addr<SentryProcessorActor> {
        let (
            protocol,
            pub_key,
            host,
            project,
        ) = scan_fmt!(secret_link, "{}://{}@{}/{}", String, String, String, i32).unwrap();

        SentryProcessorActor::create(|_| SentryProcessorActor {
            dsn: Dsn {
                protocol,
                pub_key,
                host,
                project,
            },
            client: Client::new(),
        })
    }
}

impl Actor for SentryProcessorActor {
    type Context = Context<Self>;
}

impl Handler<GelfProcessorMessage> for SentryProcessorActor {
    type Result = ();

    fn handle(&mut self, msg: GelfProcessorMessage, ctx: &mut Self::Context) -> Self::Result {
        let url = self.dsn.prepare_url();
        let gelf_msg = msg.0.as_gelf();
        let level = match gelf_msg.level {
            0 => "fatal",
            1 => "error",
            2 => "error",
            3 => "error",
            4 => "warning",
            5 => "warning",
            6 => "info",
            7 => "debug",
            _ => {
                eprintln!("unknown gelf level error: {}", gelf_msg.level);
                return;
            }
        };

        let mut data = gelf_msg.meta
           .iter()
           .map(|(k, v)| json!({ "value": v, "type": k,}))
           .collect::<Vec<Value>>();

        data.push(json!({
            "value": gelf_msg.short_message,
            "type": "GelfException",
            "mechanism": {
                "type": "generic",
                "data": gelf_msg.mechanism_data
            }
        }));

        let request = json!({
            "event_id": uuid::Uuid::new_v4(),
            "server_name": gelf_msg.host,
            "timestamp": gelf_msg.timestamp,
            "level": level,
            "exception": {
                "values": data
            }
        });

        let req = self.client
            .post(url.as_str())
            .json(request.borrow());

        ctx.spawn(async move {
            match req.send().await {
                Ok(r) => {
                    println!("sentry response: {}", r.text().await.unwrap());
                    return
                },
                Err(e) => {
                    eprintln!("request sending for sentry error: {:?}", e);
                    return
                }
            }
        }.into_actor(self));
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
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        )
    }
}