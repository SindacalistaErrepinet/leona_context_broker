//! TUI application state and input handling.
use std::sync::{Arc, Mutex};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use reqwest::Client;

use crate::{
    action,
    model::{EventLevel, MonitorState},
};

/// Top-level monitor tabs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    Topology,
    Nodes,
    Traffic,
    Events,
}

impl Tab {
    /// Ordered tab list used by the header.
    pub const ALL: [Tab; 4] = [Tab::Topology, Tab::Nodes, Tab::Traffic, Tab::Events];

    /// Tab display title.
    pub fn title(self) -> &'static str {
        match self {
            Self::Topology => "Topology",
            Self::Nodes => "Nodes",
            Self::Traffic => "Traffic",
            Self::Events => "Events",
        }
    }

    /// Index in `ALL`.
    pub fn index(self) -> usize {
        Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0)
    }

    /// Next tab with wraparound.
    pub fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    /// Previous tab with wraparound.
    pub fn previous(self) -> Self {
        let index = self.index();
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// Inline payload editor state.
pub struct PayloadEditor {
    pub text: String,
    pub cursor: usize,
}

impl PayloadEditor {
    /// Creates an editor positioned at the end of the payload.
    pub fn new(text: String) -> Self {
        let cursor = text.len();
        Self { text, cursor }
    }

    fn insert(&mut self, ch: char) {
        self.text.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let previous = self.text[..self.cursor]
            .char_indices()
            .last()
            .map(|(index, _)| index)
            .unwrap_or(0);
        self.text.remove(previous);
        self.cursor = previous;
    }

    fn move_left(&mut self) {
        if let Some((index, _)) = self.text[..self.cursor].char_indices().last() {
            self.cursor = index;
        }
    }

    fn move_right(&mut self) {
        if self.cursor < self.text.len()
            && let Some(ch) = self.text[self.cursor..].chars().next()
        {
            self.cursor += ch.len_utf8();
        }
    }
}

/// TUI application state.
pub struct App {
    pub state: Arc<Mutex<MonitorState>>,
    pub client: Client,
    pub tab: Tab,
    pub selected: usize,
    pub editor: Option<PayloadEditor>,
    pub should_quit: bool,
    pub help_visible: bool,
    pub last_action: String,
}

impl App {
    /// Creates the TUI application around shared monitor state.
    pub fn new(state: Arc<Mutex<MonitorState>>, client: Client) -> Self {
        Self {
            state,
            client,
            tab: Tab::Topology,
            selected: 0,
            editor: None,
            should_quit: false,
            help_visible: false,
            last_action: String::new(),
        }
    }

    /// Number of selectable rows for the active tab.
    pub fn selection_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        match self.tab {
            Tab::Topology | Tab::Nodes => state.defra_nodes.len() + state.broker_nodes.len(),
            Tab::Traffic => state.broker_nodes.len(),
            Tab::Events => state.events.len(),
        }
    }

    /// Selected broker id and base URL when selection points at a broker.
    pub fn selected_broker(&self) -> Option<(String, String)> {
        let state = self.state.lock().unwrap();
        match self.tab {
            Tab::Traffic => state
                .broker_nodes
                .values()
                .nth(self.selected)
                .map(|node| (node.broker_id.clone(), node.base_url.clone())),
            Tab::Topology | Tab::Nodes => {
                let offset = state.defra_nodes.len();
                if self.selected < offset {
                    return None;
                }
                state
                    .broker_nodes
                    .values()
                    .nth(self.selected - offset)
                    .map(|node| (node.broker_id.clone(), node.base_url.clone()))
            }
            Tab::Events => None,
        }
    }

    /// Handles one key press.
    pub fn handle_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }

        if self.editor.is_some() {
            self.handle_editor_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
            }
            KeyCode::Tab => {
                self.tab = self.tab.next();
                self.selected = 0;
            }
            KeyCode::BackTab => {
                self.tab = self.tab.previous();
                self.selected = 0;
            }
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_previous(),
            KeyCode::Char('p') => {
                let mut state = self.state.lock().unwrap();
                state.paused = !state.paused;
                let paused = state.paused;
                state.push_event(
                    EventLevel::Info,
                    if paused {
                        "polling paused"
                    } else {
                        "polling resumed"
                    },
                );
            }
            KeyCode::Char('s') => self.send(action::generated_payload()),
            KeyCode::Char('e') => {
                let payload = self.state.lock().unwrap().payload.clone();
                self.editor = Some(PayloadEditor::new(payload));
            }
            KeyCode::Char('?') => self.help_visible = !self.help_visible,
            KeyCode::Esc => self.help_visible = false,
            _ => {}
        }
    }

    fn handle_editor_key(&mut self, key: KeyEvent) {
        let Some(editor) = self.editor.as_mut() else {
            return;
        };
        match key.code {
            KeyCode::Esc => self.editor = None,
            KeyCode::Enter => {
                if let Some(editor) = self.editor.take() {
                    self.send(editor.text);
                }
            }
            KeyCode::Backspace => editor.backspace(),
            KeyCode::Left => editor.move_left(),
            KeyCode::Right => editor.move_right(),
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                editor.insert(ch);
            }
            _ => {}
        }
    }

    fn select_next(&mut self) {
        let len = self.selection_len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        self.selected = (self.selected + 1).min(len - 1);
    }

    fn select_previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    fn send(&mut self, payload: String) {
        let broker = self.selected_broker().or_else(|| self.first_broker());
        let Some((broker_id, base_url)) = broker else {
            self.state
                .lock()
                .unwrap()
                .push_event(EventLevel::Warn, "no broker node available to send payload");
            return;
        };

        {
            let mut state = self.state.lock().unwrap();
            state.payload = payload.clone();
            state.push_event(EventLevel::Info, format!("sending payload to {broker_id}"));
        }
        self.last_action = format!("payload -> {broker_id}");
        action::spawn_send(self.client.clone(), base_url, payload, self.state.clone());
    }

    fn first_broker(&self) -> Option<(String, String)> {
        self.state
            .lock()
            .unwrap()
            .broker_nodes
            .values()
            .next()
            .map(|node| (node.broker_id.clone(), node.base_url.clone()))
    }
}
