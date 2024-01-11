use async_trait::async_trait;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;
use rand::Rng;

pub struct DataClodAuthSource;

#[async_trait]
impl AuthSource for DataClodAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();
        let password = std::env::var("DATACLOD_PASSWORD").unwrap_or(String::from("dataclod"));
        let hash_password = hash_md5_password(
            login.user().unwrap_or_default(),
            &password,
            &salt,
        );
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}
