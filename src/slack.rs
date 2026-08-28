use anyhow::Result;
use reqwest::{Client, Url};
use serde_json::json;

use crate::{Alert, TaskLabel, WatcherAppContext};

#[derive(Clone, Debug)]
pub struct SlackConfig {
    pub webhook_url: Url,
}

impl SlackConfig {
    pub(crate) async fn handle_alert<C: WatcherAppContext>(
        &self,
        client: &Client,
        alert: Alert,
        app: &C,
    ) -> Result<()> {
        match alert {
            Alert::Down { task, error } => self.send_alert(client, task, &error, app).await,
            Alert::Recovered {
                task,
                old_error,
                new_message,
            } => {
                self.send_resolved_alert(client, task, &old_error, &new_message, app)
                    .await
            }
            Alert::FirstFailure { task, error } => self.send_alert(client, task, &error, app).await,
        }
    }

    async fn send_alert<C: WatcherAppContext>(
        &self,
        client: &Client,
        task: TaskLabel,
        error: &str,
        app: &C,
    ) -> Result<()> {
        let mut context_elements = vec![format!("*Title:* {}", app.title())];
        if let Some(env) = app.environment() {
            context_elements.push(format!("*Environment:* `{}`", env));
        }
        if let Some(version) = app.build_version() {
            let short_version = version.chars().take(7).collect::<String>();
            context_elements.push(format!("*Build Version:* `{}`", short_version));
        }
        let context_text = context_elements.join("  |  ");

        let payload = json!({
            "attachments": [
                {
                    "color": "danger",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "🚨 Task Failure",
                                "emoji": true
                            }
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": format!("*Task:* `{}`", task.ident())
                                }
                            ]
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": format!("*Error:*\n```{}```", error)
                            }
                        },
                        { "type": "divider" },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": context_text
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        client
            .post(self.webhook_url.clone())
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    async fn send_resolved_alert<C: WatcherAppContext>(
        &self,
        client: &Client,
        task: TaskLabel,
        old_error: &str,
        new_message: &str,
        app: &C,
    ) -> Result<()> {
        let mut context_elements = vec![format!("*Title:* {}", app.title())];
        if let Some(env) = app.environment() {
            context_elements.push(format!("*Environment:* `{}`", env));
        }
        if let Some(version) = app.build_version() {
            let short_version = version.chars().take(7).collect::<String>();
            context_elements.push(format!("*Build Version:* `{}`", short_version));
        }
        let context_text = context_elements.join("  |  ");

        let payload = json!({
            "attachments": [
                {
                    "color": "good",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "✅ Task Recovered",
                                "emoji": true
                            }
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": format!("*Task:* `{}`", task.ident())
                                }
                            ]
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": format!("*Previous Error:*\n```{}```", old_error)
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": format!("*New Status:*\n{}", new_message)
                            }
                        },
                        { "type": "divider" },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": context_text
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        client
            .post(self.webhook_url.clone())
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
