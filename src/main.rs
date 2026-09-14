//! Binary entrypoint for broker HTTP server.
use std::io;

use actix_web::{App, HttpServer, middleware::Logger, web};
use leona_context_broker::{
    api,
    app::{entity_watch, state::AppState},
    config::AppConfig,
    persistence::defradb::DefraDbRepositories,
};

/// Boots repositories, workers, and HTTP server.
#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let config = AppConfig::from_env();
    let repositories = DefraDbRepositories::new(&config).map_err(io::Error::other)?;
    let state = AppState::new(config.clone(), repositories).map_err(io::Error::other)?;
    entity_watch::spawn(state.clone());
    let state = web::Data::new(state);
    let bind_address = format!("{}:{}", config.host, config.port);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .configure(api::configure)
    })
    .bind(&bind_address)?
    .run()
    .await
}
