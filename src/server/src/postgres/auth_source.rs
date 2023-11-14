use async_trait::async_trait;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;
use rand::Rng;

pub struct DataClotAuthSource;

#[async_trait]
impl AuthSource for DataClotAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();
        let password = std::env::var("DATACLOT_PASSWORD").unwrap_or(String::from("dataclot"));
        let hash_password = hash_md5_password(
            login.user().map(|s| s.as_str()).unwrap_or_default(),
            &password,
            &salt,
        );
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}
