//! Leona Context Broker library crate.
//!
//! Modules are split by concern: HTTP wiring in `api`, shared state in `app`,
//! request parsing in `context` and `query`, storage adapters in
//! `persistence`, domain shapes in `domain`, business logic in `services`, and
//! small helpers in `utils`.
pub mod api;
pub mod app;
pub mod config;
pub mod context;
pub mod domain;
pub mod error;
pub mod persistence;
pub mod query;
pub mod services;
pub mod utils;
