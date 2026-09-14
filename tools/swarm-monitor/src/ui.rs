//! Ratatui rendering for the swarm monitor.
use std::collections::HashMap;

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Sparkline, Table, Tabs, Wrap,
        canvas::{Canvas, Line as CanvasLine, Points},
    },
};

use crate::{
    app::{App, Tab},
    discovery::defra_display_id,
    model::{BrokerNode, DefraNode, Edge, EdgeStatus, EventLevel, MonitorState, NodeStatus},
};

const ACTIVE: Color = Color::Green;
const INACTIVE: Color = Color::Yellow;
const OFFLINE: Color = Color::Red;
const DIM: Color = Color::DarkGray;

/// Draws the complete monitor UI.
pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(3),
    ])
    .split(frame.area());

    draw_tabs(frame, app, chunks[0]);
    match app.tab {
        Tab::Topology => draw_topology(frame, app, chunks[1]),
        Tab::Nodes => draw_nodes(frame, app, chunks[1]),
        Tab::Traffic => draw_traffic(frame, app, chunks[1]),
        Tab::Events => draw_events(frame, app, chunks[1]),
    }
    draw_footer(frame, app, chunks[2]);

    if app.help_visible {
        draw_help(frame);
    }
    if app.editor.is_some() {
        draw_editor(frame, app);
    }
}

fn draw_tabs(frame: &mut Frame, app: &App, area: Rect) {
    let titles = Tab::ALL.iter().map(|tab| tab.title()).collect::<Vec<_>>();
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Leona Swarm Monitor "),
        )
        .select(app.tab.index())
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn draw_topology(frame: &mut Frame, app: &App, area: Rect) {
    let state = app.state.lock().unwrap();
    let chunks =
        Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)]).split(area);
    draw_topology_canvas(frame, &state, chunks[0]);
    draw_edges_table(frame, app, &state, chunks[1]);
}

fn draw_topology_canvas(frame: &mut Frame, state: &MonitorState, area: Rect) {
    let positions = node_positions(state);
    let mut lines = Vec::new();
    for edge in &state.edges {
        let (Some(from), Some(to)) = (positions.get(&edge.from), positions.get(&edge.to)) else {
            continue;
        };
        lines.push(CanvasLine {
            x1: from.0,
            y1: from.1,
            x2: to.0,
            y2: to.1,
            color: edge_color(edge),
        });
    }
    let nodes = state
        .defra_nodes
        .values()
        .map(|node| (defra_display_id(node), node.status))
        .chain(
            state
                .broker_nodes
                .values()
                .map(|node| (node.broker_id.clone(), node.status)),
        )
        .collect::<Vec<_>>();

    let canvas = Canvas::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Topology (DefraDB outer, broker inner) "),
        )
        .x_bounds([0.0, 100.0])
        .y_bounds([0.0, 100.0])
        .paint(|ctx| {
            for line in &lines {
                ctx.draw(line);
            }
            for (id, status) in &nodes {
                if let Some((x, y)) = positions.get(id) {
                    let color = status_color(*status);
                    ctx.draw(&Points {
                        coords: &[(*x, *y)],
                        color,
                    });
                    ctx.print(
                        *x + 1.5,
                        *y,
                        Line::from(id.clone()).style(Style::default().fg(color)),
                    );
                }
            }
        });
    frame.render_widget(canvas, area);
}

fn draw_edges_table(frame: &mut Frame, app: &App, state: &MonitorState, area: Rect) {
    let selected = selected_node_id(app, state);
    let rows = state.edges.iter().map(|edge| {
        let highlight = selected
            .as_deref()
            .is_some_and(|id| id == edge.from || id == edge.to);
        let style = if highlight {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        Row::new(vec![
            Cell::from(edge.from.clone()),
            Cell::from(edge.to.clone()),
            Cell::from(edge.kind.label()),
            Cell::from(edge.status.label()).style(Style::default().fg(edge_color(edge))),
            Cell::from(edge.docs.to_string()),
            Cell::from(format_bytes(edge.bytes)),
            Cell::from(truncate(&edge.detail, 32)),
        ])
        .style(style)
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(vec![
            "from", "to", "kind", "status", "docs", "bytes", "detail",
        ])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Edges ({}) ", state.edges.len())),
    );
    frame.render_widget(table, area);
}

fn draw_nodes(frame: &mut Frame, app: &App, area: Rect) {
    let state = app.state.lock().unwrap();
    let mut rows = Vec::new();
    let mut index = 0;

    for node in state.defra_nodes.values() {
        rows.push(defra_row(node, index == app.selected));
        index += 1;
    }
    for node in state.broker_nodes.values() {
        rows.push(broker_row(node, index == app.selected));
        index += 1;
    }

    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(18),
            Constraint::Length(30),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(13),
        ],
    )
    .header(
        Row::new(vec![
            "kind",
            "id",
            "address",
            "status",
            "uptime",
            "writes",
            "mutations",
            "notif ok/fail",
        ])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title(format!(
        " Nodes (defra {} | brokers {}) ",
        state.defra_nodes.len(),
        state.broker_nodes.len()
    )));
    frame.render_widget(table, area);
}

fn defra_row(node: &DefraNode, selected: bool) -> Row<'static> {
    let style = selection_style(selected);
    Row::new(vec![
        Cell::from("defra"),
        Cell::from(defra_display_id(node)),
        Cell::from(truncate(&node.api_base, 30)),
        Cell::from(node.status.marker()).style(Style::default().fg(status_color(node.status))),
        Cell::from(format!("peers {}", node.active_peers.len())),
        Cell::from(format!("repl {}", node.replicators.len())),
        Cell::from(truncate(node.last_error.as_deref().unwrap_or("-"), 11)),
        Cell::from(format!("{} addr", node.addresses.len())),
    ])
    .style(style)
}

fn broker_row(node: &BrokerNode, selected: bool) -> Row<'static> {
    let style = selection_style(selected);
    let address = if node.public_endpoint.is_empty() {
        node.base_url.as_str()
    } else {
        node.public_endpoint.as_str()
    };
    Row::new(vec![
        Cell::from("broker"),
        Cell::from(node.broker_id.clone()),
        Cell::from(truncate(address, 30)),
        Cell::from(node.status.marker()).style(Style::default().fg(status_color(node.status))),
        Cell::from(format_uptime(node.uptime_ms)),
        Cell::from((node.counters.entity_writes() + node.counters.temporal_writes).to_string()),
        Cell::from(node.counters.mutations_total().to_string()),
        Cell::from(format!(
            "{}/{}/{}",
            node.counters.notifications_succeeded,
            node.counters.notifications_failed,
            node.counters.notifications_attempted
        )),
    ])
    .style(style)
}

fn draw_traffic(frame: &mut Frame, app: &App, area: Rect) {
    let state = app.state.lock().unwrap();
    let chunks = Layout::vertical([
        Constraint::Length(6),
        Constraint::Min(8),
        Constraint::Min(6),
    ])
    .split(area);

    let history = state.rate_history.iter().copied().collect::<Vec<_>>();
    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" swarm mutation events/s (last 60 polls) "),
        )
        .data(&history)
        .style(Style::default().fg(Color::Cyan));
    frame.render_widget(sparkline, chunks[0]);

    let rows = state.broker_nodes.values().map(|node| {
        let selected = state
            .broker_nodes
            .values()
            .nth(app.selected)
            .map(|candidate| candidate.broker_id == node.broker_id)
            .unwrap_or(false);
        Row::new(vec![
            Cell::from(node.broker_id.clone()),
            Cell::from((node.counters.entity_writes() + node.counters.temporal_writes).to_string()),
            Cell::from(node.counters.mutations_total().to_string()),
            Cell::from(format_bytes(node.counters.mutation_bytes_total())),
            Cell::from(format!(
                "{}/{}/{}",
                node.counters.notifications_succeeded,
                node.counters.notifications_failed,
                node.counters.notifications_attempted
            )),
            Cell::from(format!(
                "{}/{}",
                node.counters.peer_batches_sent, node.counters.peer_batches_received
            )),
            Cell::from(format!(
                "{}/{}",
                node.counters.peer_batch_docs_sent, node.counters.peer_batch_docs_received
            )),
            Cell::from(format!(
                "{}/{}",
                format_bytes(node.counters.peer_batch_bytes_sent),
                format_bytes(node.counters.peer_batch_bytes_received)
            )),
            Cell::from(format!("{:.1}", node.mutation_rate)),
        ])
        .style(selection_style(selected))
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(12),
            Constraint::Length(15),
            Constraint::Length(11),
            Constraint::Length(13),
            Constraint::Length(18),
            Constraint::Length(9),
        ],
    )
    .header(
        Row::new(vec![
            "broker",
            "writes",
            "mutations",
            "mut bytes",
            "notif ok/fail/att",
            "batches o/i",
            "batch docs o/i",
            "batch bytes o/i",
            "rate/s",
        ])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title(" Brokers "));
    frame.render_widget(table, chunks[1]);

    let peer_rows = state
        .broker_nodes
        .values()
        .nth(app.selected)
        .map(|node| {
            node.peers
                .iter()
                .map(|(peer, stats)| {
                    Row::new(vec![
                        Cell::from(truncate(peer, 40)),
                        Cell::from(stats.docs_sent.to_string()),
                        Cell::from(format_bytes(stats.bytes_sent)),
                        Cell::from(stats.docs_received.to_string()),
                        Cell::from(format_bytes(stats.bytes_received)),
                        Cell::from(stats.mutations_consumed.to_string()),
                        Cell::from(format!("{}", stats.total_exchanges())),
                        Cell::from(format_age(stats.last_seen_millis)),
                    ])
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let peer_table = Table::new(
        peer_rows,
        [
            Constraint::Length(42),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec![
            "peer",
            "docs out",
            "bytes out",
            "docs in",
            "bytes in",
            "mutations",
            "total",
            "last seen",
        ])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Selected broker peer exchange "),
    );
    frame.render_widget(peer_table, chunks[2]);
}

fn draw_events(frame: &mut Frame, app: &App, area: Rect) {
    let state = app.state.lock().unwrap();
    let height = area.height.saturating_sub(2) as usize;
    let skip = state.events.len().saturating_sub(height);
    let items = state
        .events
        .iter()
        .skip(skip)
        .map(|event| {
            let (level, color) = match event.level {
                EventLevel::Info => ("INFO", Color::Gray),
                EventLevel::Warn => ("WARN", Color::Yellow),
                EventLevel::Error => ("ERR ", Color::Red),
            };
            ListItem::new(Line::from(vec![
                Span::styled(format!("{} ", event.at), Style::default().fg(DIM)),
                Span::styled(level.to_string(), Style::default().fg(color)),
                Span::raw(format!(" {}", event.message)),
            ]))
        })
        .collect::<Vec<_>>();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Events ({}) ", state.events.len())),
    );
    frame.render_widget(list, area);
}

fn draw_footer(frame: &mut Frame, app: &App, area: Rect) {
    let state = app.state.lock().unwrap();
    let status = if state.paused {
        format!("{} | PAUSED", state.status)
    } else {
        state.status.clone()
    };
    let action = if app.last_action.is_empty() {
        String::new()
    } else {
        format!(" | last: {}", app.last_action)
    };
    let keys = "q quit | Tab switch | up/down select | s send | e payload | p pause | ? help";
    let totals = format!(
        "mutations {} | peer bytes {} | defra {} | brokers {} | edges {}",
        state.total_mutations(),
        format_bytes(state.total_peer_bytes()),
        state.defra_nodes.len(),
        state.broker_nodes.len(),
        state.edges.len()
    );
    let paragraph = Paragraph::new(vec![
        Line::from(format!("{status}{action}")),
        Line::from(Span::styled(totals, Style::default().fg(Color::Cyan))),
        Line::from(Span::styled(keys, Style::default().fg(DIM))),
    ]);
    frame.render_widget(paragraph, area);
}

fn draw_help(frame: &mut Frame) {
    let area = centered_rect(60, 40, frame.area());
    frame.render_widget(Clear, area);
    let help = Paragraph::new(vec![
        Line::from("Leona swarm monitor"),
        Line::from(""),
        Line::from("q        quit"),
        Line::from("Tab      next tab, Shift+Tab previous"),
        Line::from("up/down  select node or broker"),
        Line::from("s        send generated entity payload to selected broker"),
        Line::from("e        edit payload, Enter sends, Esc cancels"),
        Line::from("p        pause/resume polling"),
        Line::from("?        toggle this help"),
        Line::from(""),
        Line::from("Discovery: DefraDB P2P multiaddrs + broker /internal/stats"),
    ])
    .block(Block::default().borders(Borders::ALL).title(" Help "))
    .wrap(Wrap { trim: true });
    frame.render_widget(help, area);
}

fn draw_editor(frame: &mut Frame, app: &App) {
    let Some(editor) = app.editor.as_ref() else {
        return;
    };
    let area = centered_rect(80, 20, frame.area());
    frame.render_widget(Clear, area);

    let cursor = editor.cursor.min(editor.text.len());
    let (before, rest) = editor.text.split_at(cursor);
    let mut after = rest.chars();
    let cursor_char = after.next().map(|ch| ch.to_string()).unwrap_or_default();
    let line = Line::from(vec![
        Span::raw(before.to_string()),
        Span::styled(
            cursor_char,
            Style::default().add_modifier(Modifier::REVERSED),
        ),
        Span::raw(after.collect::<String>()),
    ]);

    let paragraph = Paragraph::new(vec![
        line,
        Line::from(""),
        Line::from(Span::styled(
            "Enter send | Esc cancel | payload is POSTed to /ngsi-ld/v1/entities",
            Style::default().fg(DIM),
        )),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" payload ")
            .title_alignment(Alignment::Left),
    )
    .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn node_positions(state: &MonitorState) -> HashMap<String, (f64, f64)> {
    let mut positions = HashMap::new();
    let defra_count = state.defra_nodes.len().max(1) as f64;
    let broker_count = state.broker_nodes.len().max(1) as f64;

    for (index, node) in state.defra_nodes.values().enumerate() {
        let angle = std::f64::consts::TAU * index as f64 / defra_count;
        positions.insert(
            defra_display_id(node),
            (50.0 + 40.0 * angle.cos(), 50.0 + 40.0 * angle.sin()),
        );
    }
    for (index, node) in state.broker_nodes.values().enumerate() {
        let angle = std::f64::consts::TAU * index as f64 / broker_count;
        positions.insert(
            node.broker_id.clone(),
            (50.0 + 18.0 * angle.cos(), 50.0 + 18.0 * angle.sin()),
        );
    }
    positions
}

fn selected_node_id(app: &App, state: &MonitorState) -> Option<String> {
    let defra_count = state.defra_nodes.len();
    if app.selected < defra_count {
        return state
            .defra_nodes
            .values()
            .nth(app.selected)
            .map(defra_display_id);
    }
    state
        .broker_nodes
        .values()
        .nth(app.selected - defra_count)
        .map(|node| node.broker_id.clone())
}

fn selection_style(selected: bool) -> Style {
    if selected {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    }
}

fn status_color(status: NodeStatus) -> Color {
    match status {
        NodeStatus::Online => ACTIVE,
        NodeStatus::Offline => OFFLINE,
    }
}

fn edge_color(edge: &Edge) -> Color {
    match edge.status {
        EdgeStatus::Active => ACTIVE,
        EdgeStatus::Inactive | EdgeStatus::Configured => INACTIVE,
        EdgeStatus::Offline => OFFLINE,
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_age(last_seen_millis: u64) -> String {
    if last_seen_millis == 0 {
        return "-".to_string();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let age_seconds = now.saturating_sub(last_seen_millis) / 1000;
    if age_seconds < 60 {
        format!("{age_seconds}s")
    } else {
        format!("{}m", age_seconds / 60)
    }
}

fn format_uptime(uptime_ms: u64) -> String {
    let seconds = uptime_ms / 1000;
    let minutes = seconds / 60;
    let hours = minutes / 60;
    if hours > 0 {
        format!("{hours}h{:02}m", minutes % 60)
    } else if minutes > 0 {
        format!("{minutes}m{:02}s", seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let shortened = value
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>();
    format!("{shortened}~")
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use super::*;
    use crate::model::MonitorState;
    use std::sync::{Arc, Mutex};

    #[test]
    fn renders_empty_dashboard() {
        let state = Arc::new(Mutex::new(MonitorState::new("{}".to_string())));
        let app = App::new(state, reqwest::Client::new());
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).unwrap();

        terminal
            .draw(|frame| draw(frame, &app))
            .expect("render failed");

        let buffer = terminal.backend().buffer().clone();
        let content = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("Leona Swarm Monitor"));
    }

    #[test]
    fn formats_bytes_and_uptime() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(2048), "2.0 KiB");
        assert_eq!(format_uptime(90_000), "1m30s");
        assert_eq!(format_uptime(3_600_000), "1h00m");
    }
}
