use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password, ServerParameterProvider, StartupHandler};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use rand::Rng;
use tokio::sync::Mutex;

const PG_VERSION: &str = "10.0";
const DEFAULT_DATACLOD_PASSWORD: &str = "dataclod";

pub struct DataClodStartupHandler<A, P> {
    pub auth_source: Arc<A>,
    pub parameter_provider: Arc<P>,
    pub cached_password: Mutex<Vec<u8>>,
}

#[async_trait]
impl<A: AuthSource, P: ServerParameterProvider> StartupHandler for DataClodStartupHandler<A, P> {
    async fn on_startup<C>(
        &self, client: &mut C, message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);

                let login_info = LoginInfo::from_client_info(client);
                let salt_and_pass = self.auth_source.get_password(&login_info).await?;

                let salt = salt_and_pass
                    .salt()
                    .expect("Salt is required for Md5Password authentication");

                *self.cached_password.lock().await = salt_and_pass.password().to_vec();

                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::MD5Password(salt.to_vec()),
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let cached_pass = self.cached_password.lock().await;

                let login_info = LoginInfo::from_client_info(client);
                if pwd.password.as_bytes() == *cached_pass && login_info.user() == Some("postgres")
                {
                    pgwire::api::auth::finish_authentication(
                        client,
                        self.parameter_provider.as_ref(),
                    )
                    .await?
                } else {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28P01".to_owned(),
                        "Password authentication failed".to_owned(),
                    );
                    let error = ErrorResponse::from(error_info);

                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

pub struct DataClodParameterProvider;

impl ServerParameterProvider for DataClodParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            ("server_version".to_owned(), PG_VERSION.to_owned()),
            ("server_encoding".to_owned(), "UTF8".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("DateStyle".to_owned(), "ISO YMD".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
        ]))
    }
}

#[derive(Debug)]
pub struct DataClodAuthSource;

#[async_trait]
impl AuthSource for DataClodAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let salt: [u8; 4] = rand::rng().random();
        let password =
            std::env::var("DATACLOD_PASSWORD").unwrap_or(DEFAULT_DATACLOD_PASSWORD.to_owned());
        let hash_password = hash_md5_password(login.user().unwrap_or_default(), &password, &salt);
        Ok(Password::new(
            Some(salt.to_vec()),
            hash_password.as_bytes().to_vec(),
        ))
    }
}
