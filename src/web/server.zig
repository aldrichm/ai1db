const std = @import("std");
const Engine = @import("../storage/engine.zig").Engine;
const SyncManager = @import("../sync/node.zig").SyncManager;
const Parser = @import("../sql/parser.zig").Parser;
const Executor = @import("../sql/executor.zig").Executor;
const Value = @import("../types.zig").Value;

fn writeJsonString(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0...8, 11...12, 14...31 => try std.fmt.format(writer, "\\u{x:0>4}", .{c}),
            else => try writer.writeByte(c),
        }
    }
    try writer.writeByte('"');
}

pub const WebServer = struct {
    allocator: std.mem.Allocator,
    engine: *Engine,
    sync_mgr: *SyncManager,
    port: u16,
    running: bool = false,
    thread: ?std.Thread = null,

    pub fn init(allocator: std.mem.Allocator, engine: *Engine, sync_mgr: *SyncManager, port: u16) !*WebServer {
        const ws = try allocator.create(WebServer);
        ws.* = WebServer{
            .allocator = allocator,
            .engine = engine,
            .sync_mgr = sync_mgr,
            .port = port,
            .running = false,
        };
        return ws;
    }

    pub fn deinit(self: *WebServer) void {
        self.running = false;
        self.allocator.destroy(self);
    }

    pub fn start(self: *WebServer) !void {
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, serverWorker, .{self});
    }

    fn serverWorker(self: *WebServer) void {
        const address = std.net.Address.parseIp4("0.0.0.0", self.port) catch return;
        var server = address.listen(.{ .reuse_address = true }) catch return;
        defer server.deinit();

        while (self.running) {
            const conn = server.accept() catch continue;
            _ = std.Thread.spawn(.{}, handleClient, .{ self, conn.stream }) catch {
                conn.stream.close();
            };
        }
    }

    fn handleClient(self: *WebServer, stream: std.net.Stream) void {
        defer stream.close();

        var buf: [16384]u8 = undefined;
        const n = stream.read(&buf) catch return;
        if (n == 0) return;

        const request = buf[0..n];
        var lines = std.mem.splitSequence(u8, request, "\r\n");
        const req_line = lines.next() orelse return;

        var parts = std.mem.splitScalar(u8, req_line, ' ');
        const method = parts.next() orelse return;
        const path = parts.next() orelse return;

        if (std.mem.eql(u8, method, "GET") and (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, "/index.html"))) {
            self.serveDashboard(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/status")) {
            self.serveStatus(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/peers")) {
            self.servePeers(stream);
        } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/tables")) {
            self.serveTables(stream);
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/query")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveQuery(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else {
            sendError(stream, 404, "Not Found");
        }
    }

    fn serveDashboard(self: *WebServer, stream: std.net.Stream) void {
        _ = self;
        const html =
            \\<!DOCTYPE html>
            \\<html>
            \\<head>
            \\  <meta charset="utf-8">
            \\  <title>StrataDB Desktop Console</title>
            \\  <style>
            \\    :root {
            \\      --bg: #090d16;
            \\      --card: #131c2e;
            \\      --card-border: #1e293b;
            \\      --text: #f8fafc;
            \\      --text-muted: #94a3b8;
            \\      --accent: #38bdf8;
            \\      --accent-glow: rgba(56, 189, 248, 0.2);
            \\      --success: #10b981;
            \\    }
            \\    * { box-sizing: border-box; margin: 0; padding: 0; }
            \\    body {
            \\      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            \\      background: var(--bg);
            \\      color: var(--text);
            \\      padding: 24px 32px;
            \\      line-height: 1.5;
            \\    }
            \\    .header {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      border-bottom: 1px solid var(--card-border);
            \\      padding-bottom: 16px;
            \\      margin-bottom: 20px;
            \\    }
            \\    .logo-group { display: flex; align-items: center; gap: 12px; }
            \\    .logo-badge {
            \\      background: linear-gradient(135deg, #0284c7, #38bdf8);
            \\      color: #000;
            \\      font-weight: 800;
            \\      font-size: 13px;
            \\      padding: 4px 10px;
            \\      border-radius: 6px;
            \\      letter-spacing: 0.05em;
            \\    }
            \\    h1 { font-size: 22px; font-weight: 700; color: #fff; }
            \\    .status-group { display: flex; align-items: center; gap: 12px; }
            \\    .peer-badge {
            \\      display: inline-flex;
            \\      align-items: center;
            \\      gap: 6px;
            \\      padding: 5px 12px;
            \\      border-radius: 20px;
            \\      font-size: 12px;
            \\      font-weight: 600;
            \\      background: rgba(16, 185, 129, 0.15);
            \\      color: #34d399;
            \\      border: 1px solid rgba(16, 185, 129, 0.3);
            \\    }
            \\    .dot { width: 8px; height: 8px; border-radius: 50%; background: #34d399; }
            \\    .grid {
            \\      display: grid;
            \\      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            \\      gap: 14px;
            \\      margin-bottom: 20px;
            \\    }
            \\    .card {
            \\      background: var(--card);
            \\      border: 1px solid var(--card-border);
            \\      border-radius: 8px;
            \\      padding: 14px 16px;
            \\    }
            \\    .card h3 {
            \\      font-size: 11px;
            \\      text-transform: uppercase;
            \\      letter-spacing: 0.08em;
            \\      color: var(--text-muted);
            \\      margin-bottom: 6px;
            \\    }
            \\    .metric { font-size: 17px; font-weight: 700; color: #fff; }
            \\    .submetric { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
            \\
            \\    .content-layout {
            \\      display: grid;
            \\      grid-template-columns: 1fr 340px;
            \\      gap: 20px;
            \\    }
            \\    @media (max-width: 1024px) {
            \\      .content-layout { grid-template-columns: 1fr; }
            \\    }
            \\
            \\    .template-bar {
            \\      margin-bottom: 12px;
            \\      display: flex;
            \\      flex-wrap: wrap;
            \\      align-items: center;
            \\      gap: 6px;
            \\    }
            \\    .template-bar-label {
            \\      font-size: 11px;
            \\      font-weight: 700;
            \\      text-transform: uppercase;
            \\      color: #94a3b8;
            \\      margin-right: 4px;
            \\    }
            \\    .pill {
            \\      background: #1e293b;
            \\      border: 1px solid #334155;
            \\      color: #cbd5e1;
            \\      padding: 4px 10px;
            \\      border-radius: 14px;
            \\      font-size: 12px;
            \\      font-family: monospace;
            \\      cursor: pointer;
            \\      transition: all 0.15s ease;
            \\      user-select: none;
            \\    }
            \\    .pill:hover {
            \\      background: #334155;
            \\      color: #fff;
            \\      border-color: var(--accent);
            \\    }
            \\    .pill-alter {
            \\      border-color: rgba(56, 189, 248, 0.4);
            \\      background: rgba(56, 189, 248, 0.08);
            \\      color: #7dd3fc;
            \\    }
            \\    .pill-alter:hover {
            \\      background: rgba(56, 189, 248, 0.2);
            \\      border-color: #38bdf8;
            \\      color: #fff;
            \\    }
            \\
            \\    .editor-container {
            \\      position: relative;
            \\      margin-bottom: 12px;
            \\    }
            \\    textarea#sqlInput {
            \\      width: 100%;
            \\      height: 110px;
            \\      background: #050811;
            \\      border: 1px solid #334155;
            \\      color: #f1f5f9;
            \\      padding: 12px 14px;
            \\      font-family: monospace;
            \\      font-size: 14px;
            \\      line-height: 1.5;
            \\      border-radius: 8px;
            \\      outline: none;
            \\      resize: vertical;
            \\    }
            \\    textarea#sqlInput:focus {
            \\      border-color: var(--accent);
            \\      box-shadow: 0 0 0 2px var(--accent-glow);
            \\    }
            \\
            \\    .typeahead-menu {
            \\      position: absolute;
            \\      left: 0;
            \\      bottom: -6px;
            \\      transform: translateY(100%);
            \\      width: 100%;
            \\      max-height: 260px;
            \\      background: #0b1120;
            \\      border: 1px solid #38bdf8;
            \\      border-radius: 8px;
            \\      overflow-y: auto;
            \\      z-index: 1000;
            \\      box-shadow: 0 12px 30px rgba(0, 0, 0, 0.8);
            \\    }
            \\    .typeahead-item {
            \\      display: flex;
            \\      align-items: center;
            \\      justify-content: space-between;
            \\      padding: 7px 12px;
            \\      cursor: pointer;
            \\      border-bottom: 1px solid #1e293b;
            \\    }
            \\    .typeahead-item:last-child { border-bottom: none; }
            \\    .typeahead-item.selected, .typeahead-item:hover {
            \\      background: #1e293b;
            \\    }
            \\    .item-left {
            \\      display: flex;
            \\      align-items: center;
            \\      gap: 8px;
            \\    }
            \\    .item-tag {
            \\      font-size: 10px;
            \\      font-weight: 800;
            \\      padding: 2px 6px;
            \\      border-radius: 4px;
            \\    }
            \\    .tag-kw { background: #1e3a8a; color: #93c5fd; }
            \\    .tag-tab { background: #78350f; color: #fde047; }
            \\    .tag-col { background: #064e3b; color: #6ee7b7; }
            \\    .tag-typ { background: #581c87; color: #d8b4fe; }
            \\    .tag-snip { background: #831843; color: #f472b6; }
            \\    .item-text {
            \\      font-family: monospace;
            \\      font-size: 13px;
            \\      color: #f8fafc;
            \\      font-weight: 600;
            \\    }
            \\    .item-desc {
            \\      font-size: 11px;
            \\      color: #94a3b8;
            \\    }
            \\
            \\    .action-bar {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      margin-top: 8px;
            \\    }
            \\    .btn-group { display: flex; gap: 8px; }
            \\    button.btn-primary {
            \\      background: var(--accent);
            \\      color: #04121e;
            \\      font-weight: 700;
            \\      border: none;
            \\      padding: 8px 18px;
            \\      border-radius: 6px;
            \\      cursor: pointer;
            \\      font-size: 13px;
            \\    }
            \\    button.btn-secondary {
            \\      background: #1e293b;
            \\      color: #cbd5e1;
            \\      font-weight: 600;
            \\      border: 1px solid #334155;
            \\      padding: 8px 14px;
            \\      border-radius: 6px;
            \\      cursor: pointer;
            \\      font-size: 13px;
            \\    }
            \\    .shortcut-hint {
            \\      font-size: 12px;
            \\      color: var(--text-muted);
            \\    }
            \\    kbd {
            \\      background: #1e293b;
            \\      border: 1px solid #334155;
            \\      border-radius: 3px;
            \\      padding: 2px 5px;
            \\      font-size: 11px;
            \\      font-family: monospace;
            \\    }
            \\
            \\    #queryOutput { margin-top: 16px; }
            \\    .alert-box {
            \\      padding: 10px 14px;
            \\      border-radius: 6px;
            \\      font-size: 13px;
            \\      margin-bottom: 12px;
            \\      font-family: monospace;
            \\      white-space: pre-wrap;
            \\    }
            \\    .alert-success {
            \\      background: rgba(16, 185, 129, 0.12);
            \\      border: 1px solid rgba(16, 185, 129, 0.35);
            \\      color: #6ee7b7;
            \\    }
            \\    .alert-error {
            \\      background: rgba(239, 68, 68, 0.12);
            \\      border: 1px solid rgba(239, 68, 68, 0.35);
            \\      color: #fca5a5;
            \\    }
            \\    .table-container {
            \\      overflow-x: auto;
            \\      border: 1px solid var(--card-border);
            \\      border-radius: 6px;
            \\    }
            \\    table {
            \\      width: 100%;
            \\      border-collapse: collapse;
            \\      font-size: 13px;
            \\      text-align: left;
            \\    }
            \\    th {
            \\      background: #1e293b;
            \\      color: #94a3b8;
            \\      text-transform: uppercase;
            \\      font-size: 11px;
            \\      letter-spacing: 0.05em;
            \\      padding: 9px 12px;
            \\      border-bottom: 1px solid #334155;
            \\    }
            \\    td {
            \\      padding: 8px 12px;
            \\      border-bottom: 1px solid #1e293b;
            \\      color: #e2e8f0;
            \\      font-family: monospace;
            \\    }
            \\    tr:hover td { background: rgba(56, 189, 248, 0.04); }
            \\
            \\    .schema-card {
            \\      background: var(--card);
            \\      border: 1px solid var(--card-border);
            \\      border-radius: 8px;
            \\      padding: 16px;
            \\    }
            \\    .schema-card h3 {
            \\      font-size: 11px;
            \\      text-transform: uppercase;
            \\      letter-spacing: 0.08em;
            \\      color: var(--text-muted);
            \\      margin-bottom: 12px;
            \\    }
            \\    .schema-table-item {
            \\      margin-bottom: 14px;
            \\      border-bottom: 1px solid #1e293b;
            \\      padding-bottom: 10px;
            \\    }
            \\    .schema-table-item:last-child { border-bottom: none; padding-bottom: 0; }
            \\    .schema-table-name {
            \\      display: flex;
            \\      align-items: center;
            \\      justify-content: space-between;
            \\      font-weight: 700;
            \\      font-size: 13px;
            \\      color: #38bdf8;
            \\      cursor: pointer;
            \\      margin-bottom: 6px;
            \\    }
            \\    .schema-table-name:hover { text-decoration: underline; }
            \\    .schema-col-list {
            \\      display: flex;
            \\      flex-direction: column;
            \\      gap: 3px;
            \\      padding-left: 6px;
            \\    }
            \\    .schema-col-item {
            \\      display: flex;
            \\      align-items: center;
            \\      justify-content: space-between;
            \\      font-size: 12px;
            \\      font-family: monospace;
            \\      color: #cbd5e1;
            \\      padding: 2px 6px;
            \\      border-radius: 4px;
            \\      cursor: pointer;
            \\    }
            \\    .schema-col-item:hover {
            \\      background: #1e293b;
            \\      color: #fff;
            \\    }
            \\    .pk-badge {
            \\      font-size: 9px;
            \\      font-weight: 800;
            \\      background: #b45309;
            \\      color: #fef3c7;
            \\      padding: 1px 4px;
            \\      border-radius: 3px;
            \\      margin-left: 6px;
            \\    }
            \\    .type-pill {
            \\      font-size: 11px;
            \\      color: #94a3b8;
            \\    }
            \\  </style>
            \\</head>
            \\<body>
            \\  <div class="header">
            \\    <div class="logo-group">
            \\      <div class="logo-badge">STRATADB</div>
            \\      <h1>Distributed Relational Engine</h1>
            \\    </div>
            \\    <div class="status-group">
            \\      <span id="nodeBadge" class="peer-badge"><span class="dot"></span> NODE ONLINE</span>
            \\    </div>
            \\  </div>
            \\
            \\  <div class="grid">
            \\    <div class="card">
            \\      <h3>Desktop Node Info</h3>
            \\      <div id="desktopInfo" class="metric">Loading...</div>
            \\      <div class="submetric">Peer TCP 7433 &bull; UDP 7432</div>
            \\    </div>
            \\    <div class="card">
            \\      <h3>Connected Desktop Peers</h3>
            \\      <div id="peerCount" class="metric">0 Desktops</div>
            \\      <div class="submetric">Mesh Auto-Sync Enabled</div>
            \\    </div>
            \\    <div class="card">
            \\      <h3>HLC Physical/Logical Clock</h3>
            \\      <div id="hlcTime" class="metric" style="font-size: 15px;">...</div>
            \\      <div class="submetric">Causal CRDT Consistency</div>
            \\    </div>
            \\    <div class="card">
            \\      <h3>Active Tables / CDC</h3>
            \\      <div id="statsCount" class="metric">...</div>
            \\      <div class="submetric">WAL Engine Active</div>
            \\    </div>
            \\  </div>
            \\
            \\  <div class="content-layout">
            \\    <div class="card">
            \\      <h3>SQL Query Console (with IntelliSense & Typeahead)</h3>
            \\
            \\      <div class="template-bar">
            \\        <span class="template-bar-label">Snippets:</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users MODIFY COLUMN age REAL;')">ALTER: Modify Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users ADD COLUMN email TEXT;')">ALTER: Add Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users DROP COLUMN email;')">ALTER: Drop Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users RENAME COLUMN name TO full_name;')">ALTER: Rename Col</span>
            \\        <span class="pill" onclick="insertSnippet('SELECT * FROM users;')">SELECT *</span>
            \\        <span class="pill" onclick="insertSnippet('SHOW TABLES;')">SHOW TABLES</span>
            \\        <span class="pill" onclick="insertSnippet('STATUS;')">STATUS</span>
            \\        <span class="pill" onclick="insertSnippet('PEERS;')">PEERS</span>
            \\        <span class="pill" onclick="insertSnippet('HELP;')">HELP</span>
            \\      </div>
            \\
            \\      <div class="editor-container">
            \\        <textarea id="sqlInput" spellcheck="false" autocomplete="off" placeholder="-- Type SQL statement (e.g. ALTER TABLE users MODIFY COLUMN age REAL;)..."></textarea>
            \\        <div id="typeaheadBox" class="typeahead-menu" style="display: none;"></div>
            \\      </div>
            \\
            \\      <div class="action-bar">
            \\        <div class="btn-group">
            \\          <button class="btn-primary" onclick="runQuery()">Execute Query</button>
            \\          <button class="btn-secondary" onclick="clearConsole()">Clear</button>
            \\        </div>
            \\        <div class="shortcut-hint">
            \\          <span>IntelliSense: <kbd>Tab</kbd> / <kbd>&crarr;</kbd> complete &bull; <kbd>&uarr;&darr;</kbd> navigate &bull; <kbd>Esc</kbd> dismiss &bull; <kbd>Ctrl+&crarr;</kbd> run</span>
            \\        </div>
            \\      </div>
            \\
            \\      <div id="queryOutput"></div>
            \\    </div>
            \\
            \\    <div class="schema-card">
            \\      <h3>Live Schema Explorer</h3>
            \\      <div id="schemaList" style="font-size: 13px; color: #94a3b8;">Loading tables...</div>
            \\    </div>
            \\  </div>
            \\
            \\  <script>
            \\    let knownTables = [];
            \\    let selectedTypeaheadIndex = 0;
            \\    let currentTypeaheadItems = [];
            \\    const sqlInput = document.getElementById('sqlInput');
            \\    const typeaheadBox = document.getElementById('typeaheadBox');
            \\
            \\    const SQL_KEYWORDS = [
            \\      { label: 'ALTER TABLE', type: 'kw', desc: 'Modify table structure' },
            \\      { label: 'MODIFY COLUMN', type: 'kw', desc: 'Alter column data type' },
            \\      { label: 'ADD COLUMN', type: 'kw', desc: 'Add new column to table' },
            \\      { label: 'DROP COLUMN', type: 'kw', desc: 'Remove column from table' },
            \\      { label: 'RENAME COLUMN', type: 'kw', desc: 'Rename column' },
            \\      { label: 'SELECT', type: 'kw', desc: 'Query rows from table' },
            \\      { label: 'FROM', type: 'kw', desc: 'Specify source table' },
            \\      { label: 'WHERE', type: 'kw', desc: 'Filter criteria' },
            \\      { label: 'ORDER BY', type: 'kw', desc: 'Sort query results' },
            \\      { label: 'ASC', type: 'kw', desc: 'Ascending sort' },
            \\      { label: 'DESC', type: 'kw', desc: 'Descending sort' },
            \\      { label: 'LIMIT', type: 'kw', desc: 'Constrain output rows' },
            \\      { label: 'INSERT INTO', type: 'kw', desc: 'Insert new row' },
            \\      { label: 'VALUES', type: 'kw', desc: 'Row values tuple' },
            \\      { label: 'UPDATE', type: 'kw', desc: 'Update existing rows' },
            \\      { label: 'SET', type: 'kw', desc: 'Assign column values' },
            \\      { label: 'DELETE FROM', type: 'kw', desc: 'Delete rows from table' },
            \\      { label: 'CREATE TABLE', type: 'kw', desc: 'Create new table' },
            \\      { label: 'PRIMARY KEY', type: 'kw', desc: 'Primary key constraint' },
            \\      { label: 'SHOW TABLES', type: 'kw', desc: 'List active tables' },
            \\      { label: 'DESCRIBE', type: 'kw', desc: 'Inspect table schema' },
            \\      { label: 'STATUS', type: 'kw', desc: 'Node status & CDC count' },
            \\      { label: 'PEERS', type: 'kw', desc: 'Desktop mesh peers' },
            \\      { label: 'SYNC', type: 'kw', desc: 'Connect to desktop peer' },
            \\      { label: 'HELP', type: 'kw', desc: 'List SQL command syntax' },
            \\      { label: 'COUNT', type: 'kw', desc: 'Aggregate count' },
            \\      { label: 'SUM', type: 'kw', desc: 'Aggregate sum' },
            \\      { label: 'AVG', type: 'kw', desc: 'Aggregate average' },
            \\      { label: 'MIN', type: 'kw', desc: 'Minimum value' },
            \\      { label: 'MAX', type: 'kw', desc: 'Maximum value' }
            \\    ];
            \\
            \\    const SQL_TYPES = [
            \\      { label: 'INTEGER', type: 'typ', desc: '64-bit integer' },
            \\      { label: 'REAL', type: 'typ', desc: '64-bit float' },
            \\      { label: 'TEXT', type: 'typ', desc: 'UTF-8 string' },
            \\      { label: 'BOOLEAN', type: 'typ', desc: 'true or false' },
            \\      { label: 'TIMESTAMP', type: 'typ', desc: 'Unix timestamp' },
            \\      { label: 'BLOB', type: 'typ', desc: 'Binary data' }
            \\    ];
            \\
            \\    const SQL_SNIPPETS = [
            \\      { label: 'ALTER TABLE ... MODIFY COLUMN', insertText: 'ALTER TABLE users MODIFY COLUMN age REAL;', type: 'snip', desc: 'Snippet: modify column type' },
            \\      { label: 'ALTER TABLE ... ADD COLUMN', insertText: 'ALTER TABLE users ADD COLUMN email TEXT;', type: 'snip', desc: 'Snippet: add column' },
            \\      { label: 'ALTER TABLE ... DROP COLUMN', insertText: 'ALTER TABLE users DROP COLUMN email;', type: 'snip', desc: 'Snippet: drop column' },
            \\      { label: 'ALTER TABLE ... RENAME COLUMN', insertText: 'ALTER TABLE users RENAME COLUMN name TO full_name;', type: 'snip', desc: 'Snippet: rename column' },
            \\      { label: 'CREATE TABLE ...', insertText: 'CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);', type: 'snip', desc: 'Snippet: create table' },
            \\      { label: 'SELECT * FROM ...', insertText: 'SELECT * FROM users;', type: 'snip', desc: 'Snippet: select all' },
            \\      { label: 'INSERT INTO ...', insertText: 'INSERT INTO users VALUES (1, \'Alice\', 30);', type: 'snip', desc: 'Snippet: insert row' }
            \\    ];
            \\
            \\    async function updateStatus() {
            \\      try {
            \\        const res = await fetch('/api/status');
            \\        const data = await res.json();
            \\        document.getElementById('desktopInfo').innerText = data.desktop_name + ' (' + data.node_id.substring(0,8) + ')';
            \\        document.getElementById('hlcTime').innerText = data.hlc;
            \\        document.getElementById('statsCount').innerText = data.tables_count + ' Tables &bull; ' + data.cdc_count + ' CDC Logs';
            \\      } catch(e){}
            \\      try {
            \\        const res = await fetch('/api/peers');
            \\        const peers = await res.json();
            \\        document.getElementById('peerCount').innerText = peers.length + ' Desktop(s) Connected';
            \\      } catch(e){}
            \\    }
            \\
            \\    async function fetchTables() {
            \\      try {
            \\        const res = await fetch('/api/tables');
            \\        knownTables = await res.json();
            \\        renderSchemaList();
            \\      } catch(e){}
            \\    }
            \\
            \\    function renderSchemaList() {
            \\      const container = document.getElementById('schemaList');
            \\      if (!knownTables || knownTables.length === 0) {
            \\        container.innerHTML = '<em>No tables active yet.</em>';
            \\        return;
            \\      }
            \\      let html = '';
            \\      for (const tbl of knownTables) {
            \\        html += '<div class="schema-table-item">';
            \\        html += '<div class="schema-table-name" onclick="insertSnippet(\'SELECT * FROM ' + tbl.name + ';\')"><span>&bull; ' + tbl.name + '</span><span style=\"font-size:11px;color:#94a3b8\">' + tbl.columns.length + ' cols</span></div>';
            \\        html += '<div class="schema-col-list">';
            \\        for (const col of tbl.columns) {
            \\          html += '<div class="schema-col-item" onclick="insertSnippet(\'ALTER TABLE ' + tbl.name + ' MODIFY COLUMN ' + col.name + ' ' + col.type + ';\')">';
            \\          html += '<span>' + col.name + (col.primary_key ? '<span class=\"pk-badge\">PK</span>' : '') + '</span>';
            \\          html += '<span class="type-pill">' + col.type + '</span>';
            \\          html += '</div>';
            \\        }
            \\        html += '</div></div>';
            \\      }
            \\      container.innerHTML = html;
            \\    }
            \\
            \\    function insertSnippet(text) {
            \\      sqlInput.value = text;
            \\      sqlInput.focus();
            \\      hideTypeahead();
            \\    }
            \\
            \\    function clearConsole() {
            \\      sqlInput.value = '';
            \\      document.getElementById('queryOutput').innerHTML = '';
            \\      sqlInput.focus();
            \\      hideTypeahead();
            \\    }
            \\
            \\    function hideTypeahead() {
            \\      typeaheadBox.style.display = 'none';
            \\      currentTypeaheadItems = [];
            \\    }
            \\
            \\    function getCompletions() {
            \\      const val = sqlInput.value;
            \\      const pos = sqlInput.selectionStart;
            \\      const before = val.slice(0, pos);
            \\
            \\      const match = before.match(/([a-zA-Z0-9_]+)$/);
            \\      const currentWord = match ? match[1] : '';
            \\      const prefix = before.slice(0, before.length - currentWord.length).trimEnd();
            \\      const tokens = prefix.split(/\s+/).filter(Boolean);
            \\      const lastToken = tokens.length > 0 ? tokens[tokens.length - 1].toUpperCase() : '';
            \\      const secondLast = tokens.length > 1 ? tokens[tokens.length - 2].toUpperCase() : '';
            \\      const thirdLast = tokens.length > 2 ? tokens[tokens.length - 3].toUpperCase() : '';
            \\
            \\      let candidates = [];
            \\
            \\      if (secondLast === 'ALTER' && lastToken === 'TABLE' && currentWord === '') {
            \\        for (const t of knownTables) {
            \\          candidates.push({ label: t.name, type: 'tab', desc: 'Table (' + t.columns.length + ' cols)' });
            \\        }
            \\      } else if (thirdLast === 'ALTER' && secondLast === 'TABLE') {
            \\        candidates.push({ label: 'MODIFY COLUMN', type: 'kw', desc: 'Change column data type' });
            \\        candidates.push({ label: 'ADD COLUMN', type: 'kw', desc: 'Add new column' });
            \\        candidates.push({ label: 'DROP COLUMN', type: 'kw', desc: 'Drop column' });
            \\        candidates.push({ label: 'RENAME COLUMN', type: 'kw', desc: 'Rename column' });
            \\      } else if ((secondLast === 'MODIFY' && lastToken === 'COLUMN') || (secondLast === 'DROP' && lastToken === 'COLUMN') || (secondLast === 'RENAME' && lastToken === 'COLUMN')) {
            \\        let tblName = null;
            \\        for (let i = 0; i < tokens.length - 1; i++) {
            \\          if (tokens[i].toUpperCase() === 'ALTER' && tokens[i+1].toUpperCase() === 'TABLE' && i + 2 < tokens.length) {
            \\            tblName = tokens[i+2];
            \\            break;
            \\          }
            \\        }
            \\        const targetTbl = knownTables.find(t => t.name.toLowerCase() === (tblName || '').toLowerCase());
            \\        if (targetTbl) {
            \\          for (const col of targetTbl.columns) {
            \\            candidates.push({ label: col.name, type: 'col', desc: col.type + (col.primary_key ? ' (PK)' : '') + ' in ' + targetTbl.name });
            \\          }
            \\        } else {
            \\          for (const t of knownTables) {
            \\            for (const col of t.columns) {
            \\              candidates.push({ label: col.name, type: 'col', desc: col.type + ' in ' + t.name });
            \\            }
            \\          }
            \\        }
            \\      } else if ((thirdLast === 'MODIFY' && secondLast === 'COLUMN') || (thirdLast === 'ADD' && secondLast === 'COLUMN')) {
            \\        candidates = candidates.concat(SQL_TYPES);
            \\      } else if (lastToken === 'FROM' || lastToken === 'INTO' || lastToken === 'UPDATE' || lastToken === 'DESCRIBE') {
            \\        for (const t of knownTables) {
            \\          candidates.push({ label: t.name, type: 'tab', desc: 'Table' });
            \\        }
            \\      } else if (lastToken === 'WHERE' || lastToken === 'SET' || lastToken === 'SELECT') {
            \\        for (const t of knownTables) {
            \\          for (const col of t.columns) {
            \\            candidates.push({ label: col.name, type: 'col', desc: col.type + ' in ' + t.name });
            \\          }
            \\        }
            \\        if (lastToken === 'SELECT') {
            \\          candidates.unshift({ label: '*', type: 'kw', desc: 'All columns' });
            \\        }
            \\      } else if (lastToken === 'ALTER') {
            \\        candidates.push({ label: 'TABLE', type: 'kw', desc: 'Alter table schema' });
            \\      }
            \\
            \\      candidates = candidates.concat(SQL_KEYWORDS);
            \\      candidates = candidates.concat(SQL_TYPES);
            \\      candidates = candidates.concat(SQL_SNIPPETS);
            \\      for (const t of knownTables) {
            \\        candidates.push({ label: t.name, type: 'tab', desc: 'Table' });
            \\      }
            \\
            \\      const seen = new Set();
            \\      const filtered = [];
            \\      const query = currentWord.toUpperCase();
            \\
            \\      for (const item of candidates) {
            \\        const key = item.label.toUpperCase();
            \\        if (seen.has(key)) continue;
            \\        seen.add(key);
            \\
            \\        if (!query) {
            \\          filtered.push(item);
            \\        } else {
            \\          if (key.startsWith(query)) {
            \\            filtered.push({ ...item, score: 2 });
            \\          } else if (key.includes(query)) {
            \\            filtered.push({ ...item, score: 1 });
            \\          }
            \\        }
            \\      }
            \\
            \\      if (query) {
            \\        filtered.sort((a, b) => (b.score || 0) - (a.score || 0));
            \\      }
            \\
            \\      return { items: filtered.slice(0, 10), currentWord };
            \\    }
            \\
            \\    function renderTypeahead() {
            \\      const res = getCompletions();
            \\      currentTypeaheadItems = res.items;
            \\      if (currentTypeaheadItems.length === 0) {
            \\        hideTypeahead();
            \\        return;
            \\      }
            \\      if (selectedTypeaheadIndex >= currentTypeaheadItems.length) {
            \\        selectedTypeaheadIndex = 0;
            \\      }
            \\      let html = '';
            \\      for (let i = 0; i < currentTypeaheadItems.length; i++) {
            \\        const it = currentTypeaheadItems[i];
            \\        const isSel = (i === selectedTypeaheadIndex) ? ' selected' : '';
            \\        html += '<div class="typeahead-item' + isSel + '" onmousedown="selectCompletion(' + i + ')">';
            \\        html += '<div class="item-left">';
            \\        html += '<span class="item-tag tag-' + it.type + '">' + it.type.toUpperCase() + '</span>';
            \\        html += '<span class="item-text">' + it.label + '</span>';
            \\        html += '</div>';
            \\        html += '<span class="item-desc">' + (it.desc || '') + '</span>';
            \\        html += '</div>';
            \\      }
            \\      typeaheadBox.innerHTML = html;
            \\      typeaheadBox.style.display = 'block';
            \\    }
            \\
            \\    function selectCompletion(idx) {
            \\      if (idx < 0 || idx >= currentTypeaheadItems.length) return;
            \\      const it = currentTypeaheadItems[idx];
            \\      const val = sqlInput.value;
            \\      const pos = sqlInput.selectionStart;
            \\      const match = val.slice(0, pos).match(/([a-zA-Z0-9_]+)$/);
            \\      const wordLen = match ? match[1].length : 0;
            \\      const start = pos - wordLen;
            \\      const insert = it.insertText || it.label;
            \\      const textToAdd = insert + (insert.endsWith(';') ? '' : ' ');
            \\      sqlInput.value = val.slice(0, start) + textToAdd + val.slice(pos);
            \\      const newPos = start + textToAdd.length;
            \\      sqlInput.setSelectionRange(newPos, newPos);
            \\      sqlInput.focus();
            \\      hideTypeahead();
            \\    }
            \\
            \\    sqlInput.addEventListener('input', () => {
            \\      renderTypeahead();
            \\    });
            \\
            \\    sqlInput.addEventListener('keydown', (e) => {
            \\      if (typeaheadBox.style.display !== 'none' && currentTypeaheadItems.length > 0) {
            \\        if (e.key === 'ArrowDown') {
            \\          e.preventDefault();
            \\          selectedTypeaheadIndex = (selectedTypeaheadIndex + 1) % currentTypeaheadItems.length;
            \\          renderTypeahead();
            \\          return;
            \\        } else if (e.key === 'ArrowUp') {
            \\          e.preventDefault();
            \\          selectedTypeaheadIndex = (selectedTypeaheadIndex - 1 + currentTypeaheadItems.length) % currentTypeaheadItems.length;
            \\          renderTypeahead();
            \\          return;
            \\        } else if (e.key === 'Enter' || e.key === 'Tab') {
            \\          if (!e.ctrlKey && !e.metaKey) {
            \\            e.preventDefault();
            \\            selectCompletion(selectedTypeaheadIndex);
            \\            return;
            \\          }
            \\        } else if (e.key === 'Escape') {
            \\          e.preventDefault();
            \\          hideTypeahead();
            \\          return;
            \\        }
            \\      }
            \\      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            \\        e.preventDefault();
            \\        runQuery();
            \\      }
            \\    });
            \\
            \\    document.addEventListener('click', (e) => {
            \\      if (!sqlInput.contains(e.target) && !typeaheadBox.contains(e.target)) {
            \\        hideTypeahead();
            \\      }
            \\    });
            \\
            \\    async function runQuery() {
            \\      hideTypeahead();
            \\      const sql = sqlInput.value.trim();
            \\      if (!sql) return;
            \\      const t0 = performance.now();
            \\      try {
            \\        const res = await fetch('/api/query', {
            \\          method: 'POST',
            \\          headers: {'Content-Type': 'application/json'},
            \\          body: JSON.stringify({query: sql})
            \\        });
            \\        const t1 = performance.now();
            \\        const dur = (t1 - t0).toFixed(1);
            \\        const result = await res.json();
            \\
            \\        if (result.error) {
            \\          document.getElementById('queryOutput').innerHTML =
            \\            '<div class="alert-box alert-error">Error: ' + result.error + ' (' + dur + 'ms)</div>';
            \\          return;
            \\        }
            \\
            \\        let html = '';
            \\        if (result.message) {
            \\          html += '<div class="alert-box alert-success">' + result.message + ' (' + dur + 'ms)</div>';
            \\        }
            \\        if (result.columns && result.columns.length > 0) {
            \\          html += '<div class="table-container"><table><thead><tr>';
            \\          for (const col of result.columns) {
            \\            html += '<th>' + col + '</th>';
            \\          }
            \\          html += '</tr></thead><tbody>';
            \\          for (const row of result.rows) {
            \\            html += '<tr>';
            \\            for (const val of row) {
            \\              html += '<td>' + (val === null ? '<em style=\"color:#64748b\">NULL</em>' : val) + '</td>';
            \\            }
            \\            html += '</tr>';
            \\          }
            \\          html += '</tbody></table></div>';
            \\        }
            \\        document.getElementById('queryOutput').innerHTML = html;
            \\        fetchTables();
            \\        updateStatus();
            \\      } catch(e) {
            \\        document.getElementById('queryOutput').innerHTML =
            \\          '<div class="alert-box alert-error">Network / Execution error: ' + e.message + '</div>';
            \\      }
            \\    }
            \\
            \\    setInterval(updateStatus, 2000);
            \\    setInterval(fetchTables, 3000);
            \\    updateStatus();
            \\    fetchTables();
            \\  </script>
            \\</body>
            \\</html>
        ;

        const writer = stream.writer();
        writer.print(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ html.len, html },
        ) catch {};
    }

    fn serveStatus(self: *WebServer, stream: std.net.Stream) void {
        var node_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.engine.node_id)}) catch "";

        const hlc_str = self.engine.hlc.toStringAlloc(self.allocator) catch "unknown";
        defer if (!std.mem.eql(u8, hlc_str, "unknown")) self.allocator.free(hlc_str);

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();

        std.fmt.format(json_buf.writer(),
            \\{{"node_id":"{s}","desktop_name":"{s}","hlc":"{s}","tables_count":{d},"cdc_count":{d}}}
        , .{
            node_hex,
            self.engine.desktop_name,
            hlc_str,
            self.engine.tables.count(),
            self.engine.cdc_log.count(),
        }) catch return;

        const writer = stream.writer();
        writer.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn servePeers(self: *WebServer, stream: std.net.Stream) void {
        const peers = self.sync_mgr.getPeerList(self.allocator) catch {
            sendError(stream, 500, "Internal Server Error");
            return;
        };
        defer {
            for (peers) |p| {
                self.allocator.free(p.desktop_name);
                self.allocator.free(p.address);
            }
            self.allocator.free(peers);
        }

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();

        const writer = json_buf.writer();
        writer.writeAll("[") catch return;
        for (peers, 0..) |p, i| {
            if (i > 0) writer.writeAll(",") catch return;
            writer.print(
                \\{{"node_id":"{s}","desktop_name":"{s}","address":"{s}","status":"{s}"}}
            , .{ p.node_id_hex, p.desktop_name, p.address, p.status }) catch return;
        }
        writer.writeAll("]") catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn serveTables(self: *WebServer, stream: std.net.Stream) void {
        self.engine.mutex.lock();
        defer self.engine.mutex.unlock();

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();

        writer.writeAll("[") catch return;
        var table_it = self.engine.tables.iterator();
        var t_idx: usize = 0;
        while (table_it.next()) |entry| {
            if (t_idx > 0) writer.writeAll(",") catch return;
            t_idx += 1;
            const table = entry.value_ptr.*;
            writer.writeAll("{\"name\":") catch return;
            writeJsonString(writer, table.name) catch return;
            writer.writeAll(",\"columns\":[") catch return;
            for (table.schema.columns, 0..) |col, c_idx| {
                if (c_idx > 0) writer.writeAll(",") catch return;
                writer.writeAll("{\"name\":") catch return;
                writeJsonString(writer, col.name) catch return;
                writer.print(",\"type\":\"{s}\",\"primary_key\":{s}}}", .{
                    col.data_type.name(),
                    if (col.primary_key) "true" else "false",
                }) catch return;
            }
            writer.writeAll("]}") catch return;
        }
        writer.writeAll("]") catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn serveQuery(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        var query_str: ?[]const u8 = null;

        const ParsedPayload = struct {
            query: []const u8,
        };
        if (std.json.parseFromSlice(ParsedPayload, arena_alloc, body, .{ .ignore_unknown_fields = true })) |parsed| {
            query_str = parsed.value.query;
        } else |_| {
            if (std.mem.indexOf(u8, body, "\"query\":")) |q_idx| {
                const after_q = body[q_idx + 8 ..];
                if (std.mem.indexOfScalar(u8, after_q, '"')) |quote_start| {
                    const rest = after_q[quote_start + 1 ..];
                    if (std.mem.lastIndexOfScalar(u8, rest, '"')) |quote_end| {
                        query_str = rest[0..quote_end];
                    }
                }
            }
        }

        if (query_str == null or query_str.?.len == 0) {
            sendError(stream, 400, "Missing 'query' field");
            return;
        }

        var parser = Parser.init(arena_alloc, query_str.?) catch {
            sendError(stream, 400, "Lexer error");
            return;
        };
        defer parser.deinit();

        const stmt = parser.parse() catch |err| {
            var err_buf: [128]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "SQL syntax error: {s}", .{@errorName(err)}) catch "SQL syntax error";
            sendError(stream, 400, err_msg);
            return;
        };

        var executor = Executor.init(self.allocator, self.engine);
        var res = executor.execute(stmt) catch |err| {
            var err_buf: [128]u8 = undefined;
            const err_msg = std.fmt.bufPrint(&err_buf, "Execution error: {s}", .{@errorName(err)}) catch "Execution error";
            sendError(stream, 500, err_msg);
            return;
        };
        defer res.deinit();

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();

        writer.writeAll("{\"columns\":[") catch return;
        for (res.columns, 0..) |col, i| {
            if (i > 0) writer.writeAll(",") catch return;
            writeJsonString(writer, col) catch return;
        }
        writer.writeAll("],\"rows\":[") catch return;
        for (res.rows, 0..) |row, r_idx| {
            if (r_idx > 0) writer.writeAll(",") catch return;
            writer.writeAll("[") catch return;
            for (row, 0..) |val, c_idx| {
                if (c_idx > 0) writer.writeAll(",") catch return;
                const v_str = val.toStringAlloc(self.allocator) catch "NULL";
                defer if (!std.mem.eql(u8, v_str, "NULL")) self.allocator.free(v_str);
                writeJsonString(writer, v_str) catch return;
            }
            writer.writeAll("]") catch return;
        }
        writer.writeAll("]") catch return;
        if (res.message) |msg| {
            writer.writeAll(",\"message\":") catch return;
            writeJsonString(writer, msg) catch return;
        }
        writer.writeAll("}") catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn sendError(stream: std.net.Stream, code: u16, msg: []const u8) void {
        const writer = stream.writer();
        writer.print(
            "HTTP/1.1 {d} Error\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{{\"error\":\"{s}\"}}",
            .{ code, msg },
        ) catch {};
    }
};
