//! Leona swarm monitor TUI entrypoint.
mod action;
mod app;
mod client;
mod discovery;
mod model;
mod ui;

use std::{
    io::{self, Stdout},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

use crate::{
    app::App,
    client::{PollConfig, http_client, run_poller},
    model::MonitorState,
};

/// Command line options for the swarm monitor.
#[derive(Parser, Debug)]
#[command(
    name = "leona-swarm-monitor",
    about = "Ratatui monitor for connections and data exchange in a dynamic Leona broker swarm"
)]
struct Args {
    /// DefraDB API base URLs used to seed discovery (comma separated, repeatable).
    #[arg(
        long = "defradb",
        value_delimiter = ',',
        default_value = "http://127.0.0.1:19181"
    )]
    defradb: Vec<String>,
    /// Broker base URLs used to seed discovery (comma separated, repeatable).
    #[arg(
        long = "broker",
        value_delimiter = ',',
        default_value = "http://127.0.0.1:18081"
    )]
    broker: Vec<String>,
    /// DefraDB HTTP API port used for peers discovered from multiaddrs.
    #[arg(long, default_value_t = 9181)]
    defradb_http_port: u16,
    /// Broker HTTP port used for co-located brokers of discovered DefraDB nodes.
    #[arg(long, default_value_t = 8080)]
    broker_port: u16,
    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = 2000)]
    poll_ms: u64,
    /// Per-request timeout in milliseconds.
    #[arg(long, default_value_t = 1500)]
    timeout_ms: u64,
    /// Offline poll cycles before a node is removed from the dashboard.
    #[arg(long, default_value_t = 5)]
    offline_grace: u32,
}

/// Terminal guard restoring the console on drop.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        );
        let _ = self.terminal.show_cursor();
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let timeout = Duration::from_millis(args.timeout_ms.max(100));
    let client = http_client(timeout)?;
    let state = Arc::new(Mutex::new(MonitorState::new(action::generated_payload())));

    let config = PollConfig {
        defra_seeds: args.defradb.clone(),
        broker_seeds: args.broker.clone(),
        defradb_http_port: args.defradb_http_port,
        broker_port: args.broker_port,
        interval: Duration::from_millis(args.poll_ms.max(250)),
        offline_grace: args.offline_grace,
    };
    tokio::spawn(run_poller(state.clone(), config, client.clone()));

    let mut guard = TerminalGuard::new()?;
    let mut app = App::new(state, client);

    loop {
        guard.terminal.draw(|frame| ui::draw(frame, &app))?;

        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                app.handle_key(key);
            }
        }

        if app.should_quit {
            break;
        }

        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    Ok(())
}
