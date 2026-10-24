const std = @import("std");
const Engine = @import("../storage/engine.zig").Engine;
const Table = @import("../storage/table.zig").Table;
const SyncManager = @import("../sync/node.zig").SyncManager;
const Parser = @import("../sql/parser.zig").Parser;
const Executor = @import("../sql/executor.zig").Executor;
const types = @import("../types.zig");
const Value = types.Value;
const Row = types.Row;
const MariaDbImporter = @import("../compat/mariadb.zig").MariaDbImporter;

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

    fn parseContentLength(headers: []const u8) ?usize {
        var lines = std.mem.splitSequence(u8, headers, "\r\n");
        while (lines.next()) |line| {
            if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
                const val_part = std.mem.trim(u8, line[15..], " \t");
                return std.fmt.parseInt(usize, val_part, 10) catch null;
            }
        }
        return null;
    }

    fn handleClient(self: *WebServer, stream: std.net.Stream) void {
        defer stream.close();

        var initial_buf: [16384]u8 = undefined;
        const n0 = stream.read(&initial_buf) catch return;
        if (n0 == 0) return;

        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |b| self.allocator.free(b);

        var request: []const u8 = initial_buf[0..n0];

        if (std.mem.indexOf(u8, initial_buf[0..n0], "\r\n\r\n")) |body_idx| {
            if (parseContentLength(initial_buf[0 .. body_idx + 2])) |content_len| {
                const target_total = body_idx + 4 + content_len;
                if (target_total > n0 and content_len <= 64 * 1024 * 1024) {
                    const buf_mem = self.allocator.alloc(u8, target_total) catch null;
                    if (buf_mem) |mem| {
                        @memcpy(mem[0..n0], initial_buf[0..n0]);
                        var cur: usize = n0;
                        while (cur < target_total) {
                            const chunk_n = stream.read(mem[cur..target_total]) catch 0;
                            if (chunk_n == 0) break;
                            cur += chunk_n;
                        }
                        heap_buf = mem;
                        request = mem[0..cur];
                    }
                }
            }
        }

        var lines = std.mem.splitSequence(u8, request, "\r\n");
        const req_line = lines.next() orelse return;

        var parts = std.mem.splitScalar(u8, req_line, ' ');
        const method = parts.next() orelse return;
        const path = parts.next() orelse return;

        if (std.mem.eql(u8, method, "OPTIONS")) {
            const swriter = stream.writer();
            swriter.print("HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nConnection: close\r\n\r\n", .{}) catch {};
            return;
        } else if (std.mem.eql(u8, method, "GET") and (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, "/index.html"))) {
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
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/create")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveCreateTable(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/save_metadata")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveSaveMetadata(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/add_column")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveAddColumn(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/modify_column")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveModifyColumn(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/rename_column")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveRenameColumn(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/drop_column")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveDropColumn(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/drop")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveDropTable(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/set_primary_key")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveSetPrimaryKey(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/tables/drop_primary_key")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveDropPrimaryKey(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/table_data/query")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveTableDataQuery(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/table_data/count")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveTableDataCount(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/table_data/insert")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveTableDataInsert(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/table_data/update")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveTableDataUpdate(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/table_data/delete")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveTableDataDelete(stream, body);
            } else {
                sendError(stream, 400, "Bad Request");
            }
        } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/import/table")) {
            if (std.mem.indexOf(u8, request, "\r\n\r\n")) |body_idx| {
                const body = request[body_idx + 4 ..];
                self.serveImportTable(stream, body);
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
            \\  <script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
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
            \\      --danger: #ef4444;
            \\      --warning: #f59e0b;
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
            \\    /* Navigation Tabs */
            \\    .nav-tabs {
            \\      display: flex;
            \\      gap: 8px;
            \\      margin-bottom: 20px;
            \\      border-bottom: 1px solid var(--card-border);
            \\      padding-bottom: 8px;
            \\    }
            \\    .nav-tab {
            \\      background: transparent;
            \\      border: 1px solid transparent;
            \\      color: var(--text-muted);
            \\      padding: 8px 18px;
            \\      border-radius: 8px;
            \\      font-size: 13px;
            \\      font-weight: 600;
            \\      cursor: pointer;
            \\      display: flex;
            \\      align-items: center;
            \\      gap: 8px;
            \\      transition: all 0.15s ease;
            \\    }
            \\    .nav-tab:hover {
            \\      color: #fff;
            \\      background: rgba(255, 255, 255, 0.04);
            \\    }
            \\    .nav-tab.active {
            \\      background: #1e293b;
            \\      color: #38bdf8;
            \\      border-color: #334155;
            \\      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            \\    }
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
            \\    /* Table & Metadata Studio Layout */
            \\    .studio-layout {
            \\      display: grid;
            \\      grid-template-columns: 320px 1fr;
            \\      gap: 20px;
            \\      align-items: start;
            \\    }
            \\    @media (max-width: 1024px) {
            \\      .studio-layout { grid-template-columns: 1fr; }
            \\    }
            \\
            \\    .studio-sidebar {
            \\      padding: 16px;
            \\      max-height: calc(100vh - 240px);
            \\      display: flex;
            \\      flex-direction: column;
            \\    }
            \\    .studio-sidebar-header {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      margin-bottom: 12px;
            \\    }
            \\    .sidebar-search {
            \\      margin-bottom: 12px;
            \\    }
            \\    .studio-catalog-list {
            \\      overflow-y: auto;
            \\      display: flex;
            \\      flex-direction: column;
            \\      gap: 6px;
            \\      padding-right: 4px;
            \\    }
            \\    .studio-catalog-item {
            \\      padding: 10px 12px;
            \\      border-radius: 6px;
            \\      border: 1px solid var(--card-border);
            \\      background: rgba(15, 23, 42, 0.4);
            \\      cursor: pointer;
            \\      transition: all 0.15s ease;
            \\    }
            \\    .studio-catalog-item:hover {
            \\      background: #1e293b;
            \\      border-color: #334155;
            \\    }
            \\    .studio-catalog-item.active {
            \\      background: rgba(56, 189, 248, 0.08);
            \\      border-color: var(--accent);
            \\    }
            \\    .catalog-item-header {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      margin-bottom: 4px;
            \\    }
            \\    .catalog-item-name {
            \\      font-weight: 700;
            \\      font-size: 13px;
            \\      color: #fff;
            \\    }
            \\    .catalog-item-badges {
            \\      display: flex;
            \\      gap: 4px;
            \\    }
            \\    .catalog-item-desc {
            \\      font-size: 11px;
            \\      color: #94a3b8;
            \\      overflow: hidden;
            \\      text-overflow: ellipsis;
            \\      white-space: nowrap;
            \\    }
            \\
            \\    .studio-workspace {
            \\      display: flex;
            \\      flex-direction: column;
            \\      gap: 16px;
            \\    }
            \\    .section-header {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      border-bottom: 1px solid var(--card-border);
            \\      padding-bottom: 12px;
            \\      margin-bottom: 14px;
            \\    }
            \\
            \\    .form-group {
            \\      display: flex;
            \\      flex-direction: column;
            \\      gap: 5px;
            \\    }
            \\    .form-row {
            \\      display: flex;
            \\      gap: 12px;
            \\    }
            \\    .form-label {
            \\      font-size: 11px;
            \\      font-weight: 700;
            \\      text-transform: uppercase;
            \\      letter-spacing: 0.05em;
            \\      color: #94a3b8;
            \\    }
            \\    .form-control {
            \\      background: #050811;
            \\      border: 1px solid #334155;
            \\      color: #f1f5f9;
            \\      padding: 8px 10px;
            \\      border-radius: 6px;
            \\      font-size: 13px;
            \\      font-family: inherit;
            \\      outline: none;
            \\      transition: border-color 0.15s ease;
            \\      width: 100%;
            \\    }
            \\    .form-control:focus {
            \\      border-color: var(--accent);
            \\      box-shadow: 0 0 0 2px var(--accent-glow);
            \\    }
            \\    select.form-control {
            \\      cursor: pointer;
            \\    }
            \\    select.form-control option {
            \\      background: #0b1120;
            \\      color: #f1f5f9;
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
            \\      transition: all 0.15s ease;
            \\    }
            \\    button.btn-primary:hover {
            \\      filter: brightness(1.1);
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
            \\      transition: all 0.15s ease;
            \\    }
            \\    button.btn-secondary:hover {
            \\      background: #334155;
            \\      color: #fff;
            \\    }
            \\    button.btn-danger {
            \\      background: rgba(239, 68, 68, 0.15);
            \\      border: 1px solid rgba(239, 68, 68, 0.4);
            \\      color: #fca5a5;
            \\      font-weight: 600;
            \\      border-radius: 6px;
            \\      cursor: pointer;
            \\      transition: all 0.15s ease;
            \\      padding: 8px 14px;
            \\      font-size: 13px;
            \\    }
            \\    button.btn-danger:hover {
            \\      background: rgba(239, 68, 68, 0.3);
            \\      border-color: #ef4444;
            \\      color: #fff;
            \\    }
            \\    button.btn-danger:disabled {
            \\      opacity: 0.35;
            \\      cursor: not-allowed;
            \\    }
            \\    .btn-sm {
            \\      padding: 5px 12px !important;
            \\      font-size: 12px !important;
            \\    }
            \\    .btn-xs {
            \\      padding: 2px 7px !important;
            \\      font-size: 11px !important;
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
            \\      vertical-align: middle;
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
            \\      margin-bottom: 4px;
            \\    }
            \\    .schema-table-name span:hover { text-decoration: underline; cursor: pointer; }
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
            \\    .badge-ai {
            \\      font-size: 9px;
            \\      font-weight: 800;
            \\      background: rgba(56, 189, 248, 0.2);
            \\      color: #38bdf8;
            \\      border: 1px solid rgba(56, 189, 248, 0.4);
            \\      padding: 1px 4px;
            \\      border-radius: 3px;
            \\      margin-left: 4px;
            \\    }
            \\    .type-pill {
            \\      font-size: 11px;
            \\      color: #94a3b8;
            \\    }
            \\
            \\    .badge-type {
            \\      font-size: 11px;
            \\      background: #1e293b;
            \\      border: 1px solid #334155;
            \\      padding: 2px 8px;
            \\      border-radius: 12px;
            \\      color: #94a3b8;
            \\    }
            \\    .ddl-box {
            \\      background: #050811;
            \\      border: 1px solid #1e293b;
            \\      border-radius: 6px;
            \\      padding: 12px 14px;
            \\      font-family: monospace;
            \\      font-size: 12px;
            \\      color: #7dd3fc;
            \\      white-space: pre-wrap;
            \\      word-break: break-all;
            \\    }
            \\    .empty-state {
            \\      display: flex;
            \\      flex-direction: column;
            \\      align-items: center;
            \\      justify-content: center;
            \\      text-align: center;
            \\      padding: 60px 20px;
            \\    }
            \\
            \\    /* TableData Table Display & Editor */
            \\    .table-data-banner {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      padding: 12px 18px;
            \\      background: #0f172a;
            \\      border: 1px solid #1e293b;
            \\      border-radius: 8px;
            \\      margin-bottom: 16px;
            \\    }
            \\    .table-data-title-group {
            \\      display: flex;
            \\      align-items: center;
            \\      gap: 12px;
            \\    }
            \\    .table-data-badge {
            \\      background: linear-gradient(135deg, #0284c7, #2563eb);
            \\      color: #fff;
            \\      font-weight: 800;
            \\      font-size: 11px;
            \\      padding: 3px 8px;
            \\      border-radius: 4px;
            \\      letter-spacing: 0.08em;
            \\      font-family: monospace;
            \\    }
            \\    .table-data-top-controls {
            \\      display: grid;
            \\      grid-template-columns: 2fr 1.2fr 1fr;
            \\      gap: 16px;
            \\      margin-bottom: 16px;
            \\    }
            \\    .table-data-chips {
            \\      display: flex;
            \\      gap: 6px;
            \\      align-items: center;
            \\      margin-top: 6px;
            \\    }
            \\    .table-data-chip {
            \\      background: #1e293b;
            \\      border: 1px solid #334155;
            \\      color: #94a3b8;
            \\      font-size: 11px;
            \\      padding: 2px 8px;
            \\      border-radius: 12px;
            \\      cursor: pointer;
            \\      font-weight: 600;
            \\      transition: all 0.15s ease;
            \\    }
            \\    .table-data-chip:hover, .table-data-chip.active {
            \\      background: #0284c7;
            \\      border-color: #38bdf8;
            \\      color: #fff;
            \\    }
            \\    .table-data-criteria-table {
            \\      width: 100%;
            \\      border-collapse: collapse;
            \\      font-size: 13px;
            \\    }
            \\    .table-data-criteria-table th {
            \\      background: #0f172a;
            \\      color: #94a3b8;
            \\      font-size: 11px;
            \\      font-weight: 700;
            \\      text-transform: uppercase;
            \\      letter-spacing: 0.05em;
            \\      padding: 8px 12px;
            \\      border-bottom: 2px solid #334155;
            \\    }
            \\    .table-data-criteria-table td {
            \\      padding: 6px 10px;
            \\      border-bottom: 1px solid #1e293b;
            \\      vertical-align: middle;
            \\    }
            \\    .table-data-criteria-table tr:hover td {
            \\      background: rgba(56, 189, 248, 0.03);
            \\    }
            \\    .table-data-op-select {
            \\      background: #050811;
            \\      border: 1px solid #334155;
            \\      color: #f1f5f9;
            \\      font-size: 12px;
            \\      font-family: monospace;
            \\      padding: 4px 6px;
            \\      border-radius: 4px;
            \\      width: 100%;
            \\      min-width: 90px;
            \\    }
            \\    .table-data-val-input {
            \\      background: #050811;
            \\      border: 1px solid #334155;
            \\      color: #f1f5f9;
            \\      font-size: 12px;
            \\      padding: 4px 8px;
            \\      border-radius: 4px;
            \\      width: 100%;
            \\      box-sizing: border-box;
            \\    }
            \\    .table-data-val-input:focus, .table-data-op-select:focus {
            \\      border-color: var(--accent);
            \\      outline: none;
            \\    }
            \\    .alv-toolbar {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      flex-wrap: wrap;
            \\      gap: 10px;
            \\      padding: 10px 14px;
            \\      background: #0f172a;
            \\      border: 1px solid #1e293b;
            \\      border-radius: 8px 8px 0 0;
            \\      border-bottom: none;
            \\    }
            \\    .alv-btn-group {
            \\      display: flex;
            \\      align-items: center;
            \\      gap: 6px;
            \\      flex-wrap: wrap;
            \\    }
            \\    .alv-btn {
            \\      display: inline-flex;
            \\      align-items: center;
            \\      gap: 6px;
            \\      background: #1e293b;
            \\      color: #cbd5e1;
            \\      border: 1px solid #334155;
            \\      padding: 6px 12px;
            \\      border-radius: 5px;
            \\      font-size: 12px;
            \\      font-weight: 600;
            \\      cursor: pointer;
            \\      transition: all 0.15s ease;
            \\    }
            \\    .alv-btn:hover:not(:disabled) {
            \\      background: #334155;
            \\      color: #fff;
            \\      border-color: #475569;
            \\    }
            \\    .alv-btn:disabled {
            \\      opacity: 0.4;
            \\      cursor: not-allowed;
            \\    }
            \\    .alv-btn.primary {
            \\      background: #0284c7;
            \\      border-color: #0369a1;
            \\      color: #fff;
            \\    }
            \\    .alv-btn.primary:hover:not(:disabled) {
            \\      background: #0369a1;
            \\    }
            \\    .alv-btn.danger {
            \\      background: rgba(239, 68, 68, 0.15);
            \\      border-color: rgba(239, 68, 68, 0.4);
            \\      color: #fca5a5;
            \\    }
            \\    .alv-btn.danger:hover:not(:disabled) {
            \\      background: rgba(239, 68, 68, 0.3);
            \\      color: #fff;
            \\    }
            \\    .alv-btn.warning {
            \\      background: rgba(245, 158, 11, 0.15);
            \\      border-color: rgba(245, 158, 11, 0.4);
            \\      color: #fcd34d;
            \\    }
            \\    .alv-btn.warning:hover:not(:disabled) {
            \\      background: rgba(245, 158, 11, 0.3);
            \\      color: #fff;
            \\    }
            \\    .alv-mode-toggle {
            \\      background: #0b1120;
            \\      border: 1px solid #334155;
            \\      border-radius: 6px;
            \\      padding: 3px;
            \\      display: inline-flex;
            \\      gap: 4px;
            \\    }
            \\    .alv-mode-btn {
            \\      background: transparent;
            \\      border: none;
            \\      color: #94a3b8;
            \\      font-size: 11px;
            \\      font-weight: 700;
            \\      padding: 4px 10px;
            \\      border-radius: 4px;
            \\      cursor: pointer;
            \\      transition: all 0.15s ease;
            \\    }
            \\    .alv-mode-btn.active {
            \\      background: #1e293b;
            \\      color: #fff;
            \\      box-shadow: 0 1px 3px rgba(0,0,0,0.4);
            \\    }
            \\    .alv-mode-btn.active.edit-active {
            \\      background: #d97706;
            \\      color: #000;
            \\      font-weight: 800;
            \\    }
            \\    .table-data-edit-banner {
            \\      background: linear-gradient(90deg, #78350f, #92400e);
            \\      border-left: 4px solid #f59e0b;
            \\      color: #fef3c7;
            \\      padding: 8px 16px;
            \\      font-size: 12px;
            \\      font-weight: 600;
            \\      display: flex;
            \\      align-items: center;
            \\      justify-content: space-between;
            \\    }
            \\    .table-data-grid-container {
            \\      overflow: auto;
            \\      max-height: 560px;
            \\      border: 1px solid #1e293b;
            \\      background: #090d16;
            \\    }
            \\    .table-data-grid-table {
            \\      width: 100%;
            \\      border-collapse: collapse;
            \\      font-size: 13px;
            \\    }
            \\    .table-data-grid-table thead {
            \\      position: sticky;
            \\      top: 0;
            \\      z-index: 10;
            \\    }
            \\    .table-data-grid-table th {
            \\      background: #131c2e;
            \\      color: #94a3b8;
            \\      font-size: 11px;
            \\      font-weight: 700;
            \\      letter-spacing: 0.05em;
            \\      padding: 8px 12px;
            \\      border-bottom: 2px solid #334155;
            \\      border-right: 1px solid #1e293b;
            \\      cursor: pointer;
            \\      user-select: none;
            \\      white-space: nowrap;
            \\    }
            \\    .table-data-grid-table th:hover {
            \\      background: #1e293b;
            \\      color: #38bdf8;
            \\    }
            \\    .table-data-grid-table th.th-pk {
            \\      border-top: 2px solid #38bdf8;
            \\      color: #38bdf8;
            \\    }
            \\    .table-data-grid-table td {
            \\      padding: 6px 12px;
            \\      border-bottom: 1px solid #1e293b;
            \\      border-right: 1px solid #131c2e;
            \\      font-family: monospace;
            \\      font-size: 12px;
            \\      white-space: nowrap;
            \\      color: #cbd5e1;
            \\    }
            \\    .table-data-grid-table td.td-pk {
            \\      font-weight: 700;
            \\      color: #38bdf8;
            \\      background: rgba(56, 189, 248, 0.02);
            \\    }
            \\    .table-data-grid-table tr:hover td {
            \\      background: rgba(56, 189, 248, 0.06);
            \\    }
            \\    .table-data-grid-table tr.selected td {
            \\      background: rgba(2, 132, 199, 0.22) !important;
            \\      color: #fff;
            \\    }
            \\    .table-data-null-badge {
            \\      display: inline-block;
            \\      padding: 1px 5px;
            \\      font-size: 10px;
            \\      font-weight: 700;
            \\      color: #64748b;
            \\      background: rgba(100, 116, 139, 0.15);
            \\      border-radius: 3px;
            \\      font-style: italic;
            \\    }
            \\    .table-data-pk-icon {
            \\      color: #f59e0b;
            \\      font-size: 12px;
            \\      margin-right: 4px;
            \\    }
            \\    .table-data-status-bar {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      padding: 8px 16px;
            \\      background: #0f172a;
            \\      border: 1px solid #1e293b;
            \\      border-radius: 0 0 8px 8px;
            \\      font-size: 12px;
            \\      color: #94a3b8;
            \\    }
            \\    .table-data-modal-backdrop {
            \\      position: fixed;
            \\      top: 0;
            \\      left: 0;
            \\      width: 100vw;
            \\      height: 100vh;
            \\      background: rgba(3, 7, 18, 0.75);
            \\      backdrop-filter: blur(4px);
            \\      display: flex;
            \\      align-items: center;
            \\      justify-content: center;
            \\      z-index: 9999;
            \\    }
            \\    .table-data-modal {
            \\      background: #0f172a;
            \\      border: 1px solid #334155;
            \\      border-radius: 10px;
            \\      width: 90%;
            \\      max-width: 680px;
            \\      max-height: 85vh;
            \\      display: flex;
            \\      flex-direction: column;
            \\      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.85);
            \\      animation: modalFadeIn 0.15s ease-out;
            \\    }
            \\    @keyframes modalFadeIn {
            \\      from { opacity: 0; transform: scale(0.97); }
            \\      to { opacity: 1; transform: scale(1); }
            \\    }
            \\    .table-data-modal-header {
            \\      display: flex;
            \\      justify-content: space-between;
            \\      align-items: center;
            \\      padding: 14px 20px;
            \\      border-bottom: 1px solid #1e293b;
            \\    }
            \\    .table-data-modal-header h3 {
            \\      font-size: 16px;
            \\      color: #fff;
            \\      display: flex;
            \\      align-items: center;
            \\      gap: 8px;
            \\    }
            \\    .table-data-modal-body {
            \\      padding: 20px;
            \\      overflow-y: auto;
            \\      flex: 1;
            \\    }
            \\    .table-data-modal-footer {
            \\      display: flex;
            \\      justify-content: flex-end;
            \\      gap: 10px;
            \\      padding: 12px 20px;
            \\      border-top: 1px solid #1e293b;
            \\      background: #0b1120;
            \\      border-radius: 0 0 10px 10px;
            \\    }
            \\    .table-data-detail-row {
            \\      display: grid;
            \\      grid-template-columns: 200px 1fr;
            \\      padding: 8px 12px;
            \\      border-bottom: 1px solid #1e293b;
            \\      font-size: 13px;
            \\      align-items: center;
            \\    }
            \\    .table-data-detail-row:nth-child(even) {
            \\      background: rgba(255, 255, 255, 0.015);
            \\    }
            \\    .table-data-detail-label {
            \\      color: #94a3b8;
            \\    }
            \\    .table-data-detail-val {
            \\      font-family: monospace;
            \\      color: #f1f5f9;
            \\      word-break: break-all;
            \\    }
            \\    /* Excel Import Modal Styles */
            \\    .excel-import-modal {
            \\      max-width: 920px;
            \\      width: 95%;
            \\    }
            \\    .import-tabs {
            \\      display: flex;
            \\      gap: 8px;
            \\      margin-bottom: 14px;
            \\      border-bottom: 1px solid var(--card-border);
            \\      padding-bottom: 8px;
            \\    }
            \\    .import-tab {
            \\      background: none;
            \\      border: 1px solid transparent;
            \\      color: var(--text-muted);
            \\      font-size: 13px;
            \\      padding: 6px 14px;
            \\      border-radius: 6px;
            \\      cursor: pointer;
            \\      font-weight: 600;
            \\      transition: all 0.15s;
            \\    }
            \\    .import-tab:hover {
            \\      color: #fff;
            \\      background: rgba(255, 255, 255, 0.05);
            \\    }
            \\    .import-tab.active {
            \\      background: rgba(56, 189, 248, 0.15);
            \\      color: var(--accent);
            \\      border-color: rgba(56, 189, 248, 0.3);
            \\    }
            \\    .excel-dropzone {
            \\      border: 2px dashed #334155;
            \\      border-radius: 8px;
            \\      padding: 28px 20px;
            \\      text-align: center;
            \\      cursor: pointer;
            \\      background: rgba(15, 23, 42, 0.6);
            \\      transition: all 0.2s ease;
            \\    }
            \\    .excel-dropzone:hover, .excel-dropzone.dragover {
            \\      border-color: var(--accent);
            \\      background: rgba(56, 189, 248, 0.08);
            \\    }
            \\    .pk-config-box {
            \\      background: rgba(56, 189, 248, 0.08);
            \\      border: 1px solid rgba(56, 189, 248, 0.28);
            \\      border-radius: 8px;
            \\      padding: 12px 16px;
            \\      margin-top: 14px;
            \\    }
            \\    .excel-preview-table {
            \\      font-size: 12px;
            \\      border-collapse: collapse;
            \\    }
            \\    .excel-preview-table th {
            \\      background: #1e293b;
            \\      color: #f1f5f9;
            \\      padding: 6px 10px;
            \\      text-align: left;
            \\      font-size: 11px;
            \\      white-space: nowrap;
            \\      border-bottom: 1px solid #334155;
            \\    }
            \\    .excel-preview-table td {
            \\      padding: 6px 10px;
            \\      border-bottom: 1px solid #1e293b;
            \\      font-family: monospace;
            \\      white-space: nowrap;
            \\      color: #cbd5e1;
            \\    }
            \\    .excel-preview-table td.col-pk, .excel-preview-table th.col-pk {
            \\      background: rgba(56, 189, 248, 0.12);
            \\      color: #38bdf8;
            \\      font-weight: 700;
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
            \\  <!-- Primary Navigation Tabs -->
            \\  <div class="nav-tabs">
            \\    <button id="tabBtnSql" class="nav-tab active" onclick="switchMainTab('sql')">
            \\      <span>⚡</span> SQL Query Console
            \\    </button>
            \\    <button id="tabBtnStudio" class="nav-tab" onclick="switchMainTab('studio')">
            \\      <span>🛠️</span> Table &amp; Metadata Studio
            \\    </button>
            \\    <button id="tabBtnTableData" class="nav-tab" onclick="switchMainTab('tableData')">
            \\      <span>📊</span> Table Data Editor
            \\    </button>
            \\  </div>
            \\
            \\  <!-- SQL Query Console View -->
            \\  <div id="sqlView" class="content-layout">
            \\    <div class="card">
            \\      <h3>SQL Query Console (with IntelliSense &amp; Typeahead)</h3>
            \\
            \\      <div class="template-bar">
            \\        <span class="template-bar-label">Snippets:</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users MODIFY COLUMN age REAL;')">ALTER: Modify Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users ADD COLUMN email TEXT;')">ALTER: Add Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users DROP COLUMN email;')">ALTER: Drop Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users RENAME COLUMN name TO full_name;')">ALTER: Rename Col</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users ADD PRIMARY KEY (id);')">ALTER: Add PK</span>
            \\        <span class="pill pill-alter" onclick="insertSnippet('ALTER TABLE users DROP PRIMARY KEY;')">ALTER: Drop PK</span>
            \\        <span class="pill pill-alter" data-snip="IMPORT MARIADB '/path/to/dump.sql';" onclick="insertSnippet(this.dataset.snip)">IMPORT MARIADB</span>
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
            \\      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            \\        <h3 style="margin-bottom: 0;">Live Schema Explorer</h3>
            \\        <button class="btn-primary btn-xs" onclick="openCreateMode()">+ New Table</button>
            \\      </div>
            \\      <div id="schemaList" style="font-size: 13px; color: #94a3b8;">Loading tables...</div>
            \\    </div>
            \\  </div>
            \\
            \\  <!-- Table & Metadata Studio View -->
            \\  <div id="studioView" class="studio-layout" style="display: none;">
            \\    <!-- Left Sidebar: Tables Catalog -->
            \\    <div class="studio-sidebar card">
            \\      <div class="studio-sidebar-header">
            \\        <div>
            \\          <h3 style="margin-bottom: 2px;">Table Catalog</h3>
            \\          <div id="studioCatalogCount" style="font-size: 11px; color: var(--text-muted);">0 Tables</div>
            \\        </div>
            \\        <div style="display: flex; gap: 6px;">
            \\          <button class="btn-secondary btn-sm" onclick="openExcelImportModal()" title="Import Table from Excel / CSV">📥 Import</button>
            \\          <button class="btn-primary btn-sm" onclick="openCreateMode()">+ New Table</button>
            \\        </div>
            \\      </div>
            \\      <div class="sidebar-search">
            \\        <input type="text" id="studioSearchInput" class="form-control" placeholder="Search tables..." oninput="filterStudioCatalog()">
            \\      </div>
            \\      <div id="studioCatalogList" class="studio-catalog-list"></div>
            \\    </div>
            \\
            \\    <!-- Right Workspace -->
            \\    <div class="studio-workspace">
            \\      <div id="studioAlert" style="display: none;" class="alert-box"></div>
            \\
            \\      <!-- Empty State -->
            \\      <div id="studioEmptyState" class="card empty-state">
            \\        <div style="font-size: 40px; margin-bottom: 12px;">🛠️</div>
            \\        <h2 style="font-size: 18px; color: #fff; margin-bottom: 8px;">Table &amp; Metadata Studio</h2>
            \\        <p style="color: var(--text-muted); font-size: 13px; max-width: 480px; margin-bottom: 20px;">
            \\          Define and amend distributed tables, data types, primary keys, and data dictionary descriptions across your cluster.
            \\        </p>
            \\        <div style="display: flex; gap: 8px; justify-content: center;">
            \\          <button class="btn-primary" onclick="openCreateMode()">+ Create New Table</button>
            \\          <button class="btn-secondary" onclick="openExcelImportModal()">📥 Import from Excel</button>
            \\        </div>
            \\      </div>
            \\
            \\      <!-- Create Mode Panel -->
            \\      <div id="studioCreatePanel" class="card" style="display: none;">
            \\        <div class="section-header">
            \\          <div>
            \\            <h2 style="font-size: 16px; color: #fff;">Create New Table</h2>
            \\            <p style="color: var(--text-muted); font-size: 12px;">Define table specifications, primary keys, and field descriptions</p>
            \\          </div>
            \\          <button class="btn-secondary btn-sm" onclick="cancelStudioEdit()">Cancel</button>
            \\        </div>
            \\
            \\        <div class="template-bar" style="margin-top: 6px;">
            \\          <span class="template-bar-label">Presets:</span>
            \\          <span class="pill" onclick="applyPreset('users')">Users Account</span>
            \\          <span class="pill" onclick="applyPreset('products')">Products Catalog</span>
            \\          <span class="pill" onclick="applyPreset('orders')">Orders &amp; Billing</span>
            \\          <span class="pill" onclick="applyPreset('metrics')">IoT Telemetry</span>
            \\        </div>
            \\
            \\        <div class="form-row" style="margin-top: 14px;">
            \\          <div class="form-group" style="flex: 1;">
            \\            <label class="form-label">Table Name *</label>
            \\            <input type="text" id="createTableName" class="form-control" placeholder="e.g. inventory_items" oninput="updateDdlPreview()">
            \\          </div>
            \\        </div>
            \\
            \\        <div class="form-group" style="margin-top: 12px;">
            \\          <label class="form-label">Table Description / Purpose</label>
            \\          <textarea id="createTableDesc" class="form-control" rows="2" placeholder="e.g. Master catalog of retail inventory items, tracking SKU, unit prices, and warehouse levels."></textarea>
            \\        </div>
            \\
            \\        <div style="display: flex; justify-content: space-between; align-items: center; margin: 20px 0 8px 0;">
            \\          <h4 style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--accent);">Field Specifications &amp; Descriptions</h4>
            \\          <button class="btn-secondary btn-sm" type="button" onclick="addCreateColumnRow()">+ Add Field</button>
            \\        </div>
            \\
            \\        <div class="table-container">
            \\          <table>
            \\            <thead>
            \\              <tr>
            \\                <th style="width: 20%;">Field Name *</th>
            \\                <th style="width: 16%;">Data Type</th>
            \\                <th style="width: 8%; text-align: center;">PK</th>
            \\                <th style="width: 8%; text-align: center;">Auto Inc</th>
            \\                <th style="width: 8%; text-align: center;">Nullable</th>
            \\                <th style="width: 32%;">Description</th>
            \\                <th style="width: 8%; text-align: center;">Action</th>
            \\              </tr>
            \\            </thead>
            \\            <tbody id="createColumnsTableBody"></tbody>
            \\          </table>
            \\        </div>
            \\
            \\        <div style="margin-top: 16px;">
            \\          <div style="font-size: 11px; text-transform: uppercase; color: #94a3b8; font-weight: 700; margin-bottom: 6px;">Real-Time DDL Preview</div>
            \\          <pre id="createDdlPreview" class="ddl-box">CREATE TABLE ...</pre>
            \\        </div>
            \\
            \\        <div style="display: flex; gap: 10px; margin-top: 18px;">
            \\          <button class="btn-primary" onclick="submitCreateTable()">Create Table &amp; Metadata</button>
            \\          <button class="btn-secondary" onclick="cancelStudioEdit()">Cancel</button>
            \\        </div>
            \\      </div>
            \\
            \\      <!-- Amend Mode Panel -->
            \\      <div id="studioAmendPanel" class="card" style="display: none;">
            \\        <div class="section-header">
            \\          <div>
            \\            <div style="display: flex; align-items: center; gap: 10px;">
            \\              <h2 id="amendTableTitle" style="font-size: 18px; color: #fff;">users</h2>
            \\              <span id="amendTableRowCount" class="badge-type">0 rows</span>
            \\              <span id="amendTableColCount" class="badge-type">0 columns</span>
            \\            </div>
            \\            <p style="color: var(--text-muted); font-size: 12px; margin-top: 2px;">Inspect specifications, amend metadata, and alter columns</p>
            \\          </div>
            \\          <div style="display: flex; gap: 8px;">
            \\            <button class="btn-primary btn-sm" onclick="openTableInTableData(currentStudioTable)">📊 Open in Data Editor</button>
            \\            <button class="btn-secondary btn-sm" onclick="queryTableInConsole()">⚡ Query in Console</button>
            \\            <button class="btn-danger btn-sm" onclick="submitDropCurrentTable()">🗑️ Drop Table</button>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Table Description & Metadata -->
            \\        <div class="card" style="background: rgba(15, 23, 42, 0.6); border: 1px solid #1e293b; margin-bottom: 18px;">
            \\          <h4 style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--accent); margin-bottom: 8px;">Table Description &amp; Purpose</h4>
            \\          <div style="display: flex; gap: 10px;">
            \\            <textarea id="amendTableDesc" class="form-control" rows="2" style="flex: 1;" placeholder="Provide a functional description of this table's purpose..."></textarea>
            \\            <button class="btn-primary btn-sm" style="align-self: flex-start; height: 38px; white-space: nowrap;" onclick="submitSaveTableDescription()">Save Description</button>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Field Specifications Catalog -->
            \\        <div>
            \\          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            \\            <h4 style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--accent);">Field Specifications Catalog</h4>
            \\            <button class="btn-secondary btn-sm" onclick="toggleAddColumnForm()">+ Add New Column</button>
            \\          </div>
            \\
            \\          <!-- Add Column Collapsible Form -->
            \\          <div id="addColumnForm" class="card" style="display: none; background: #0b1120; border: 1px solid #38bdf8; margin-bottom: 14px; padding: 14px;">
            \\            <div style="font-size: 13px; font-weight: 700; color: #38bdf8; margin-bottom: 10px;">Add New Column to Table</div>
            \\            <div style="display: grid; grid-template-columns: 1fr 120px auto auto 1.4fr auto; gap: 10px; align-items: flex-end;">
            \\              <div class="form-group">
            \\                <label class="form-label">Column Name *</label>
            \\                <input type="text" id="newColName" class="form-control" placeholder="e.g. phone_number">
            \\              </div>
            \\              <div class="form-group">
            \\                <label class="form-label">Type</label>
            \\                <select id="newColType" class="form-control" onchange="onNewColTypeChange()">
            \\                  <option value="TEXT">TEXT</option>
            \\                  <option value="INTEGER">INTEGER</option>
            \\                  <option value="REAL">REAL</option>
            \\                  <option value="BOOLEAN">BOOLEAN</option>
            \\                  <option value="TIMESTAMP">TIMESTAMP</option>
            \\                  <option value="BLOB">BLOB</option>
            \\                </select>
            \\              </div>
            \\              <div class="form-group" style="display: flex; align-items: center; gap: 6px; padding-bottom: 8px;">
            \\                <input type="checkbox" id="newColPk" style="cursor: pointer;">
            \\                <label for="newColPk" class="form-label" style="margin: 0; cursor: pointer; white-space: nowrap;">Primary Key</label>
            \\              </div>
            \\              <div class="form-group" style="display: flex; align-items: center; gap: 6px; padding-bottom: 8px;">
            \\                <input type="checkbox" id="newColAutoInc" style="cursor: pointer;" disabled>
            \\                <label for="newColAutoInc" class="form-label" style="margin: 0; cursor: pointer; white-space: nowrap;">Auto Inc</label>
            \\              </div>
            \\              <div class="form-group">
            \\                <label class="form-label">Field Description</label>
            \\                <input type="text" id="newColDesc" class="form-control" placeholder="e.g. Contact phone with country code">
            \\              </div>
            \\              <div style="display: flex; gap: 6px;">
            \\                <button class="btn-primary btn-sm" onclick="submitAddColumn()">Add</button>
            \\                <button class="btn-secondary btn-sm" onclick="toggleAddColumnForm()">Cancel</button>
            \\              </div>
            \\            </div>
            \\          </div>
            \\
            \\          <div class="table-container">
            \\            <table>
            \\              <thead>
            \\                <tr>
            \\                  <th style="width: 18%;">Column Name</th>
            \\                  <th style="width: 16%;">Data Type</th>
            \\                  <th style="width: 8%; text-align: center;">PK</th>
            \\                  <th style="width: 8%; text-align: center;">Auto Inc</th>
            \\                  <th style="width: 8%; text-align: center;">Nullable</th>
            \\                  <th style="width: 30%;">Field Description</th>
            \\                  <th style="width: 12%; text-align: center;">Actions</th>
            \\                </tr>
            \\              </thead>
            \\              <tbody id="amendColumnsTableBody"></tbody>
            \\            </table>
            \\          </div>
            \\        </div>
            \\      </div>
            \\    </div>
            \\  </div>
            \\
            \\  <!-- TableData Table Display & Data Maintenance View -->
            \\  <div id="tableDataView" style="display: none;">
            \\    <!-- 1. Selection Screen -->
            \\    <div id="tableDataSelectionScreen">
            \\      <div class="table-data-banner">
            \\        <div class="table-data-title-group">
            \\          <span class="table-data-badge">Table Editor</span>
            \\          <h2 style="font-size: 18px; color: #fff;">General Table Display</h2>
            \\          <span id="tableDataTableSummaryBadge" class="badge-type" style="display: none;"></span>
            \\        </div>
            \\        <div class="alv-btn-group">
            \\          <button class="alv-btn primary" onclick="executeTableDataQuery()" title="Execute (F8)">
            \\            <span>▶</span> Execute (F8)
            \\          </button>
            \\          <button class="alv-btn" onclick="countTableDataEntries()" title="Number of Entries (Shift+F10)">
            \\            <span>🔢</span> Number of Entries
            \\          </button>
            \\          <button class="alv-btn" onclick="resetTableDataCriteria()" title="Reset selection criteria">
            \\            <span>↺</span> Reset Criteria
            \\          </button>
            \\          <button class="alv-btn" onclick="toggleAllCriteriaDisplay(true)" title="Select all columns for output">
            \\            <span>☑</span> All Output
            \\          </button>
            \\          <button class="alv-btn" onclick="toggleAllCriteriaDisplay(false)" title="Deselect non-key columns">
            \\            <span>☐</span> Clear Output
            \\          </button>
            \\          <button class="alv-btn" onclick="jumpToStudioFromTableData()" title="Open Table Studio">
            \\            <span>🛠️</span> Table Studio
            \\          </button>
            \\          <button class="alv-btn" onclick="openExcelImportModal()" title="Import Table from Excel / CSV">
            \\            <span>📥</span> Import Excel
            \\          </button>
            \\        </div>
            \\      </div>
            \\
            \\      <div id="tableDataSelectionAlert" style="display: none;" class="alert-box"></div>
            \\
            \\      <!-- Top Controls -->
            \\      <div class="card" style="margin-bottom: 16px;">
            \\        <div class="table-data-top-controls">
            \\          <div class="form-group" style="margin-bottom: 0;">
            \\            <label class="form-label" style="font-weight: 700; color: #38bdf8;">Table Name *</label>
            \\            <select id="tableDataTableSelect" class="form-control" onchange="onTableDataTableChange(this.value)">
            \\              <option value="">-- Select a table --</option>
            \\            </select>
            \\            <div id="tableDataTableMetadataInfo" style="font-size: 12px; color: #94a3b8; margin-top: 4px;"></div>
            \\          </div>
            \\
            \\          <div class="form-group" style="margin-bottom: 0;">
            \\            <label class="form-label" style="font-weight: 700;">Maximum No. of Hits</label>
            \\            <input type="number" id="tableDataMaxHits" class="form-control" value="500" min="0" step="50">
            \\            <div class="table-data-chips">
            \\              <span class="table-data-chip" onclick="setTableDataMaxHits(50)">50</span>
            \\              <span class="table-data-chip" onclick="setTableDataMaxHits(100)">100</span>
            \\              <span class="table-data-chip active" onclick="setTableDataMaxHits(500)">500</span>
            \\              <span class="table-data-chip" onclick="setTableDataMaxHits(1000)">1000</span>
            \\              <span class="table-data-chip" onclick="setTableDataMaxHits(0)">All</span>
            \\            </div>
            \\          </div>
            \\
            \\          <div class="form-group" style="margin-bottom: 0;">
            \\            <label class="form-label" style="font-weight: 700;">Column Header Display</label>
            \\            <select id="tableDataHeaderDisplaySelect" class="form-control" onchange="onTableDataHeaderFormatChange(this.value)">
            \\              <option value="both">Both: Technical (Text)</option>
            \\              <option value="tech">Technical Name Only</option>
            \\              <option value="text">Field Description Only</option>
            \\            </select>
            \\            <div style="font-size: 11px; color: #94a3b8; margin-top: 4px;">ALV grid header presentation</div>
            \\          </div>
            \\        </div>
            \\      </div>
            \\
            \\      <!-- Selection Criteria Matrix Table -->
            \\      <div class="card">
            \\        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            \\          <h3 style="font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--accent);">
            \\            Selection Criteria &amp; Field Filters
            \\          </h3>
            \\          <span style="font-size: 12px; color: #94a3b8;">
            \\            Supports =, &lt;&gt;, &gt;, &gt;=, &lt;, &lt;=, LIKE (* or % wildcard), BETWEEN (range), IS NULL
            \\          </span>
            \\        </div>
            \\
            \\        <div class="table-container" style="max-height: 480px; overflow-y: auto;">
            \\          <table class="table-data-criteria-table">
            \\            <thead style="position: sticky; top: 0; z-index: 5;">
            \\              <tr>
            \\                <th style="width: 6%; text-align: center;" title="Output column in result table">Output</th>
            \\                <th style="width: 5%; text-align: center;" title="Key Field">Key</th>
            \\                <th style="width: 15%;">Field Name</th>
            \\                <th style="width: 22%;">Field Text (Description)</th>
            \\                <th style="width: 10%;">Data Type</th>
            \\                <th style="width: 12%;">Operator</th>
            \\                <th style="width: 15%;">Value From (Low)</th>
            \\                <th style="width: 15%;">Value To (High)</th>
            \\              </tr>
            \\            </thead>
            \\            <tbody id="tableDataCriteriaBody">
            \\              <tr>
            \\                <td colspan="8" style="text-align: center; color: #94a3b8; padding: 30px;">
            \\                  Select a table above to load selection criteria.
            \\                </td>
            \\              </tr>
            \\            </tbody>
            \\          </table>
            \\        </div>
            \\      </div>
            \\    </div>
            \\
            \\    <!-- 2. ALV Grid Screen (Output Display & Editor) -->
            \\    <div id="tableDataGridScreen" style="display: none;">
            \\      <!-- Top Breadcrumb & Status Bar -->
            \\      <div class="table-data-banner">
            \\        <div class="table-data-title-group">
            \\          <button class="alv-btn" onclick="backToTableDataSelection()" title="Back to Selection Criteria (F3)">
            \\            <span>←</span> Back to Selection (F3)
            \\          </button>
            \\          <span class="table-data-badge">Grid View</span>
            \\          <span id="tableDataGridTableTitle" style="font-size: 16px; font-weight: 700; color: #fff;"></span>
            \\          <span id="tableDataGridHitsBadge" class="badge-type"></span>
            \\        </div>
            \\        <div class="alv-btn-group">
            \\          <button class="alv-btn" onclick="refreshTableDataGrid()" title="Refresh result (F8)">
            \\            <span>🔄</span> Refresh (F8)
            \\          </button>
            \\          <button class="alv-btn" onclick="openTableDataDetailModal()" title="Single record inspector (F2)">
            \\            <span>🔍</span> Record Details (F2)
            \\          </button>
            \\          <button class="alv-btn" onclick="cycleTableDataHeaderFormat()" title="Cycle column header format">
            \\            <span>🏷️</span> Headers (<span id="tableDataHeaderModeLabel">Both</span>)
            \\          </button>
            \\          <button class="alv-btn" onclick="exportTableDataCsv()" title="Export result to CSV file">
            \\            <span>📥</span> CSV
            \\          </button>
            \\          <button class="alv-btn" onclick="exportTableDataJson()" title="Export result to JSON file">
            \\            <span>📥</span> JSON
            \\          </button>
            \\          <button class="alv-btn" onclick="copyTableDataClipboard()" title="Copy TSV to clipboard (Excel paste)">
            \\            <span>📋</span> Copy (Excel)
            \\          </button>
            \\        </div>
            \\      </div>
            \\
            \\      <!-- ALV Action Toolbar: Mode switch & Maintenance Actions -->
            \\      <div class="alv-toolbar">
            \\        <div class="alv-btn-group">
            \\          <div class="alv-mode-toggle">
            \\            <button id="tableDataModeDisplayBtn" class="alv-mode-btn active" onclick="setTableDataEditMode(false)">
            \\              👁️ Display Only
            \\            </button>
            \\            <button id="tableDataModeEditBtn" class="alv-mode-btn" onclick="setTableDataEditMode(true)" title="Toggle Edit Mode">
            \\              ✏️ Edit Mode
            \\            </button>
            \\          </div>
            \\          <button id="tableDataNewRowBtn" class="alv-btn primary" onclick="openTableDataNewModal()" disabled title="Insert new table record">
            \\            <span>+</span> New Entry
            \\          </button>
            \\          <button id="tableDataEditRowBtn" class="alv-btn warning" onclick="openTableDataEditModal()" disabled title="Edit selected row">
            \\            <span>✏️</span> Edit Selected
            \\          </button>
            \\          <button id="tableDataCloneRowBtn" class="alv-btn" onclick="cloneTableDataSelectedRow()" disabled title="Duplicate selected row">
            \\            <span>📋</span> Duplicate
            \\          </button>
            \\          <button id="tableDataDeleteRowBtn" class="alv-btn danger" onclick="deleteTableDataSelectedRows()" disabled title="Delete selected row(s)">
            \\            <span>🗑️</span> Delete Selected
            \\          </button>
            \\          <button class="alv-btn" onclick="openExcelImportModal()" title="Import Table from Excel / CSV">
            \\            <span>📥</span> Import (Excel)
            \\          </button>
            \\        </div>
            \\        <div style="display: flex; align-items: center; gap: 8px;">
            \\          <input type="text" id="tableDataGridFilterInput" class="form-control" style="width: 220px; padding: 5px 10px; font-size: 12px;" placeholder="Search in grid..." oninput="filterTableDataGridLocal()">
            \\        </div>
            \\      </div>
            \\
            \\      <!-- Edit Mode Active Warning Banner -->
            \\      <div id="tableDataEditBanner" class="table-data-edit-banner" style="display: none;">
            \\        <span>⚠️ <strong>EDIT MODE ACTIVE:</strong> Direct cluster table maintenance enabled. Changes persist with WAL/CDC/HLC causal consistency.</span>
            \\        <span style="font-size: 11px; opacity: 0.85;">StrataDB Cluster Active</span>
            \\      </div>
            \\
            \\      <div id="tableDataGridAlert" style="display: none;" class="alert-box"></div>
            \\
            \\      <!-- Interactive ALV Table Grid -->
            \\      <div class="table-data-grid-container" id="tableDataGridContainer">
            \\        <table class="table-data-grid-table" id="tableDataGridTable">
            \\          <thead id="tableDataGridThead"></thead>
            \\          <tbody id="tableDataGridTbody"></tbody>
            \\        </table>
            \\      </div>
            \\
            \\      <!-- ALV Bottom Status Bar -->
            \\      <div class="table-data-status-bar">
            \\        <div>
            \\          <span id="tableDataStatusHitsText">0 records</span>
            \\          <span style="margin: 0 8px; color: #334155;">&bull;</span>
            \\          <span id="tableDataStatusSelectedText">0 selected</span>
            \\        </div>
            \\        <div style="font-size: 11px; color: #64748b;">
            \\          Double-click row to view details &bull; Click column header to sort
            \\        </div>
            \\      </div>
            \\    </div>
            \\
            \\    <!-- Modals -->
            \\    <!-- 1. Single Record Detail Modal (F2) -->
            \\    <div id="tableDataDetailModal" class="table-data-modal-backdrop" style="display: none;">
            \\      <div class="table-data-modal">
            \\        <div class="table-data-modal-header">
            \\          <h3><span>🔍</span> Single Record Details</h3>
            \\          <div style="display: flex; align-items: center; gap: 8px;">
            \\            <button class="alv-btn" onclick="prevTableDataDetailRecord()" title="Previous Record">&lt;</button>
            \\            <span id="tableDataDetailIndexBadge" style="font-size: 12px; color: #94a3b8; font-family: monospace;">1 / 1</span>
            \\            <button class="alv-btn" onclick="nextTableDataDetailRecord()" title="Next Record">&gt;</button>
            \\            <button class="btn-secondary btn-xs" onclick="closeTableDataModal('tableDataDetailModal')">✕</button>
            \\          </div>
            \\        </div>
            \\        <div class="table-data-modal-body" id="tableDataDetailModalBody"></div>
            \\        <div class="table-data-modal-footer">
            \\          <button class="btn-secondary btn-sm" onclick="closeTableDataModal('tableDataDetailModal')">Close</button>
            \\        </div>
            \\      </div>
            \\    </div>
            \\
            \\    <!-- 2. New Entry Modal -->
            \\    <div id="tableDataNewModal" class="table-data-modal-backdrop" style="display: none;">
            \\      <div class="table-data-modal">
            \\        <div class="table-data-modal-header">
            \\          <h3><span>+</span> Insert New Table Entry</h3>
            \\          <button class="btn-secondary btn-xs" onclick="closeTableDataModal('tableDataNewModal')">✕</button>
            \\        </div>
            \\        <div class="table-data-modal-body" id="tableDataNewModalBody"></div>
            \\        <div class="table-data-modal-footer">
            \\          <button class="btn-secondary btn-sm" onclick="closeTableDataModal('tableDataNewModal')">Cancel</button>
            \\          <button class="btn-primary btn-sm" onclick="submitTableDataNewEntry()">Insert Entry</button>
            \\        </div>
            \\      </div>
            \\    </div>
            \\
            \\    <!-- 3. Edit Entry Modal -->
            \\    <div id="tableDataEditModal" class="table-data-modal-backdrop" style="display: none;">
            \\      <div class="table-data-modal">
            \\        <div class="table-data-modal-header">
            \\          <h3><span>✏️</span> Edit Table Entry</h3>
            \\          <button class="btn-secondary btn-xs" onclick="closeTableDataModal('tableDataEditModal')">✕</button>
            \\        </div>
            \\        <div class="table-data-modal-body" id="tableDataEditModalBody"></div>
            \\        <div class="table-data-modal-footer">
            \\          <button class="btn-secondary btn-sm" onclick="closeTableDataModal('tableDataEditModal')">Cancel</button>
            \\          <button class="btn-primary btn-sm" onclick="submitTableDataEditEntry()">Save Changes</button>
            \\        </div>
            \\      </div>
            \\    </div>
            \\
            \\    <!-- 4. Number of Entries Modal (Shift+F10) -->
            \\    <div id="tableDataCountModal" class="table-data-modal-backdrop" style="display: none;">
            \\      <div class="table-data-modal" style="max-width: 440px;">
            \\        <div class="table-data-modal-header">
            \\          <h3><span>🔢</span> Number of Entries</h3>
            \\          <button class="btn-secondary btn-xs" onclick="closeTableDataModal('tableDataCountModal')">✕</button>
            \\        </div>
            \\        <div class="table-data-modal-body" style="text-align: center; padding: 30px 20px;">
            \\          <div style="font-size: 13px; color: #94a3b8; margin-bottom: 8px;">Number of entries matching selection criteria:</div>
            \\          <div id="tableDataCountModalValue" style="font-size: 32px; font-weight: 800; color: #38bdf8; font-family: monospace;">0</div>
            \\          <div id="tableDataCountModalTable" style="font-size: 12px; color: #64748b; margin-top: 6px;"></div>
            \\        </div>
            \\        <div class="table-data-modal-footer" style="justify-content: center;">
            \\          <button class="btn-primary btn-sm" style="min-width: 80px;" onclick="closeTableDataModal('tableDataCountModal')">OK</button>
            \\        </div>
            \\      </div>
            \\    </div>
            \\  </div>
            \\
            \\  <!-- Excel / CSV Import Modal -->
            \\  <div id="excelImportModal" class="table-data-modal-backdrop" style="display: none;">
            \\    <div class="table-data-modal excel-import-modal">
            \\      <div class="table-data-modal-header">
            \\        <div style="display: flex; align-items: center; gap: 10px;">
            \\          <span style="font-size: 20px;">📥</span>
            \\          <div>
            \\            <h3 style="margin: 0; font-size: 16px; color: #fff;">Import Table from Excel / CSV</h3>
            \\            <div style="font-size: 11px; color: #94a3b8;">Create table and bulk-load records (Line 1: Field Names &bull; Line 2+: Data Records)</div>
            \\          </div>
            \\        </div>
            \\        <button class="btn-secondary btn-xs" onclick="closeExcelImportModal()">✕</button>
            \\      </div>
            \\
            \\      <div class="table-data-modal-body" style="padding: 16px 20px; max-height: calc(90vh - 125px); overflow-y: auto;">
            \\        <div id="excelImportAlert" class="alert-box" style="display: none; margin-bottom: 14px;"></div>
            \\
            \\        <!-- Import Source Tabs -->
            \\        <div class="import-tabs">
            \\          <button id="importTabFileBtn" class="import-tab active" onclick="switchImportSourceTab('file')">📁 Upload File (.xlsx, .xls, .csv, .tsv)</button>
            \\          <button id="importTabPasteBtn" class="import-tab" onclick="switchImportSourceTab('paste')">📋 Paste Data (from Excel / TSV)</button>
            \\        </div>
            \\
            \\        <!-- Tab 1: File Upload -->
            \\        <div id="importFileSection">
            \\          <div class="excel-dropzone" id="excelDropzone" onclick="document.getElementById('excelFileInput').click()">
            \\            <input type="file" id="excelFileInput" accept=".xlsx,.xls,.csv,.tsv,text/csv,text/tab-separated-values" style="display: none;" onchange="handleExcelFileInput(event)">
            \\            <div style="font-size: 32px; margin-bottom: 8px;">📊</div>
            \\            <div style="font-weight: 700; color: #fff; font-size: 14px; margin-bottom: 4px;">Drag &amp; drop your Excel or CSV file here, or click to browse</div>
            \\            <div style="font-size: 12px; color: #94a3b8;">Line 1 is used exclusively for field names. Data records start on Line 2. Supports Excel (.xlsx, .xls) and Delimited text (.csv, .tsv).</div>
            \\            <div id="excelFileSelectedInfo" style="margin-top: 10px; font-weight: 600; color: #38bdf8; display: none;"></div>
            \\          </div>
            \\
            \\          <div id="excelSheetSelectorRow" class="form-row" style="margin-top: 12px; display: none;">
            \\            <div class="form-group" style="margin-bottom: 0;">
            \\              <label class="form-label" style="font-weight: 700; color: #38bdf8;">Select Workbook Sheet:</label>
            \\              <select id="excelSheetSelect" class="form-control" onchange="onExcelSheetSelectChange(this.value)"></select>
            \\            </div>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Tab 2: Clipboard Paste -->
            \\        <div id="importPasteSection" style="display: none;">
            \\          <div class="form-group">
            \\            <label class="form-label" style="font-weight: 700; color: #38bdf8;">Paste Excel / Delimited Data (Ctrl+V / Cmd+V):</label>
            \\            <textarea id="excelPasteInput" class="form-control" rows="6" placeholder="Paste CSV or TSV text here... Line 1: Field names only. Line 2+: Data records." oninput="onExcelPasteInput()"></textarea>
            \\            <div style="font-size: 11px; color: #94a3b8; margin-top: 4px;">Line 1 is used exclusively for field names. Data records strictly start on Line 2.</div>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Table Destination Metadata -->
            \\        <div class="form-row" style="margin-top: 16px;">
            \\          <div class="form-group">
            \\            <label class="form-label">Target Table Name *</label>
            \\            <input type="text" id="excelImportTableName" class="form-control" placeholder="e.g. products_catalog" oninput="sanitizeImportTableName(this)">
            \\          </div>
            \\          <div class="form-group">
            \\            <label class="form-label">Table Description / Purpose</label>
            \\            <input type="text" id="excelImportTableDesc" class="form-control" placeholder="Functional description of this imported dataset...">
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Primary Key Configuration (Default 'id' PK requirement) -->
            \\        <div class="pk-config-box">
            \\          <div style="display: flex; align-items: flex-start; gap: 10px;">
            \\            <input type="checkbox" id="excelImportAddIdPk" checked onchange="toggleExcelImportIdPk(this.checked)" style="margin-top: 3px; cursor: pointer; transform: scale(1.15);">
            \\            <div>
            \\              <label for="excelImportAddIdPk" style="font-size: 13px; font-weight: 700; color: #fff; cursor: pointer; display: flex; align-items: center; gap: 6px;">
            \\                Add auto-incrementing 'id' primary key column (Recommended)
            \\                <span class="badge-type" style="background: rgba(56, 189, 248, 0.2); color: #38bdf8;">Default</span>
            \\              </label>
            \\              <div style="font-size: 12px; color: #94a3b8; margin-top: 2px;">
            \\                Prepends a unique sequential integer primary key column (<code style="color: #38bdf8;">id INTEGER PRIMARY KEY AUTO_INCREMENT</code>) to ensure all records have a cluster-wide unique identifier.
            \\              </div>
            \\            </div>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- Column Schema Specifications -->
            \\        <div id="excelImportColumnsContainer" style="margin-top: 16px; display: none;">
            \\          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            \\            <h4 style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--accent); margin: 0;">
            \\              Detected Column Schema &amp; Specifications
            \\            </h4>
            \\            <span id="excelImportColSummary" style="font-size: 11px; color: #94a3b8;"></span>
            \\          </div>
            \\          <div class="table-container" style="max-height: 220px; overflow-y: auto;">
            \\            <table>
            \\              <thead>
            \\                <tr>
            \\                  <th style="width: 20%;">Column Name</th>
            \\                  <th style="width: 18%;">Inferred Type</th>
            \\                  <th style="width: 10%; text-align: center;">Primary Key</th>
            \\                  <th style="width: 10%; text-align: center;">Auto Inc</th>
            \\                  <th style="width: 10%; text-align: center;">Nullable</th>
            \\                  <th style="width: 32%;">Field Description</th>
            \\                </tr>
            \\              </thead>
            \\              <tbody id="excelImportColumnsTableBody"></tbody>
            \\            </table>
            \\          </div>
            \\        </div>
            \\
            \\        <!-- 5-Row Live Data Preview -->
            \\        <div id="excelImportPreviewContainer" style="margin-top: 16px; display: none;">
            \\          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            \\            <h4 style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: #10b981; margin: 0;">
            \\              Live Data Preview (First 5 Rows)
            \\            </h4>
            \\            <span id="excelImportRowSummary" style="font-size: 11px; color: #94a3b8;"></span>
            \\          </div>
            \\          <div class="table-container" style="max-height: 200px; overflow-x: auto; overflow-y: auto;">
            \\            <table class="excel-preview-table">
            \\              <thead id="excelImportPreviewThead"></thead>
            \\              <tbody id="excelImportPreviewTbody"></tbody>
            \\            </table>
            \\          </div>
            \\        </div>
            \\      </div>
            \\
            \\      <div class="table-data-modal-footer">
            \\        <div id="excelImportStatusText" style="font-size: 12px; color: #94a3b8; margin-right: auto;"></div>
            \\        <button class="btn-secondary btn-sm" onclick="closeExcelImportModal()">Cancel</button>
            \\        <button id="excelImportSubmitBtn" class="btn-primary btn-sm" onclick="submitExcelImport()" disabled>Import Table &amp; Data</button>
            \\      </div>
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
            \\    let currentMainTab = 'sql';
            \\    let currentStudioTable = null;
            \\    let studioMode = null; // 'create' or 'amend'
            \\    let createColumns = [];
            \\
            \\    let tableDataTable = null;
            \\    let tableDataEditMode = false;
            \\    let tableDataHeaderFormat = 'both'; // 'both', 'tech', 'text'
            \\    let tableDataResultData = null;
            \\    let tableDataSelectedRows = new Set();
            \\    let tableDataDetailIndex = 0;
            \\    let tableDataSortCol = null;
            \\    let tableDataSortAsc = true;
            \\
            \\    const SQL_KEYWORDS = [
            \\      { label: 'ALTER TABLE', type: 'kw', desc: 'Modify table structure' },
            \\      { label: 'MODIFY COLUMN', type: 'kw', desc: 'Alter column data type' },
            \\      { label: 'ADD COLUMN', type: 'kw', desc: 'Add new column to table' },
            \\      { label: 'DROP COLUMN', type: 'kw', desc: 'Remove column from table' },
            \\      { label: 'RENAME COLUMN', type: 'kw', desc: 'Rename column' },
            \\      { label: 'IMPORT MARIADB', type: 'kw', desc: 'Import MariaDB 11.3 dump file' },
            \\      { label: 'IMPORT', type: 'kw', desc: 'Import SQL dump file' },
            \\      { label: 'MARIADB', type: 'kw', desc: 'MariaDB dialect' },
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
            \\      { label: 'AUTO_INCREMENT', type: 'kw', desc: 'Auto-incrementing integer sequence' },
            \\      { label: 'AUTOINCREMENT', type: 'kw', desc: 'Auto-incrementing sequence alias' },
            \\      { label: 'NOT NULL', type: 'kw', desc: 'Disallow NULL values' },
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
            \\      { label: 'IMPORT MARIADB ...', insertText: "IMPORT MARIADB '/path/to/dump.sql';", type: 'snip', desc: 'Snippet: import MariaDB 11.3 dump file' },
            \\      { label: 'CREATE TABLE ...', insertText: 'CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);', type: 'snip', desc: 'Snippet: create table' },
            \\      { label: 'CREATE TABLE ... AUTO_INCREMENT', insertText: 'CREATE TABLE items (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL);', type: 'snip', desc: 'Snippet: create table with auto-increment' },
            \\      { label: 'SELECT * FROM ...', insertText: 'SELECT * FROM users;', type: 'snip', desc: 'Snippet: select all' },
            \\      { label: 'INSERT INTO ...', insertText: "INSERT INTO users VALUES (1, 'Alice', 30);", type: 'snip', desc: 'Snippet: insert row' }
            \\    ];
            \\
            \\    function escapeHtml(str) {
            \\      if (str === null || str === undefined) return '';
            \\      return String(str)
            \\        .replace(/&/g, '&amp;')
            \\        .replace(/</g, '&lt;')
            \\        .replace(/>/g, '&gt;')
            \\        .replace(/"/g, '&quot;')
            \\        .replace(/'/g, '&#39;');
            \\    }
            \\
            \\    function switchMainTab(tab) {
            \\      currentMainTab = tab;
            \\      const sqlView = document.getElementById('sqlView');
            \\      const studioView = document.getElementById('studioView');
            \\      const tableDataView = document.getElementById('tableDataView');
            \\      const tabBtnSql = document.getElementById('tabBtnSql');
            \\      const tabBtnStudio = document.getElementById('tabBtnStudio');
            \\      const tabBtnTableData = document.getElementById('tabBtnTableData');
            \\
            \\      if (tab === 'sql') {
            \\        if (sqlView) sqlView.style.display = 'grid';
            \\        if (studioView) studioView.style.display = 'none';
            \\        if (tableDataView) tableDataView.style.display = 'none';
            \\        if (tabBtnSql) tabBtnSql.classList.add('active');
            \\        if (tabBtnStudio) tabBtnStudio.classList.remove('active');
            \\        if (tabBtnTableData) tabBtnTableData.classList.remove('active');
            \\      } else if (tab === 'studio') {
            \\        if (sqlView) sqlView.style.display = 'none';
            \\        if (studioView) studioView.style.display = 'grid';
            \\        if (tableDataView) tableDataView.style.display = 'none';
            \\        if (tabBtnSql) tabBtnSql.classList.remove('active');
            \\        if (tabBtnStudio) tabBtnStudio.classList.add('active');
            \\        if (tabBtnTableData) tabBtnTableData.classList.remove('active');
            \\        renderStudioCatalog();
            \\        if (!currentStudioTable && studioMode !== 'create') {
            \\          if (knownTables && knownTables.length > 0) {
            \\            openAmendMode(knownTables[0].name);
            \\          } else {
            \\            const emptyEl = document.getElementById('studioEmptyState');
            \\            const createEl = document.getElementById('studioCreatePanel');
            \\            const amendEl = document.getElementById('studioAmendPanel');
            \\            if (emptyEl) emptyEl.style.display = 'flex';
            \\            if (createEl) createEl.style.display = 'none';
            \\            if (amendEl) amendEl.style.display = 'none';
            \\          }
            \\        }
            \\      } else if (tab === 'tableData') {
            \\        if (sqlView) sqlView.style.display = 'none';
            \\        if (studioView) studioView.style.display = 'none';
            \\        if (tableDataView) tableDataView.style.display = 'flex';
            \\        if (tabBtnSql) tabBtnSql.classList.remove('active');
            \\        if (tabBtnStudio) tabBtnStudio.classList.remove('active');
            \\        if (tabBtnTableData) tabBtnTableData.classList.add('active');
            \\        updateTableDataTableSelect();
            \\      }
            \\    }
            \\
            \\    function showStudioAlert(type, message) {
            \\      const el = document.getElementById('studioAlert');
            \\      el.className = 'alert-box ' + (type === 'error' ? 'alert-error' : 'alert-success');
            \\      el.innerText = message;
            \\      el.style.display = 'block';
            \\      setTimeout(() => {
            \\        if (el.innerText === message) el.style.display = 'none';
            \\      }, 6000);
            \\    }
            \\
            \\    function openCreateMode() {
            \\      studioMode = 'create';
            \\      currentStudioTable = null;
            \\      if (currentMainTab !== 'studio') {
            \\        switchMainTab('studio');
            \\      }
            \\      const emptyEl = document.getElementById('studioEmptyState');
            \\      const createEl = document.getElementById('studioCreatePanel');
            \\      const amendEl = document.getElementById('studioAmendPanel');
            \\      if (emptyEl) emptyEl.style.display = 'none';
            \\      if (amendEl) amendEl.style.display = 'none';
            \\      if (createEl) createEl.style.display = 'block';
            \\      document.getElementById('createTableName').value = '';
            \\      document.getElementById('createTableDesc').value = '';
            \\
            \\      createColumns = [
            \\        { name: 'id', type: 'INTEGER', pk: true, auto_increment: true, nullable: false, desc: 'Primary unique identifier' },
            \\        { name: 'name', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: 'Entity name' }
            \\      ];
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\      renderStudioCatalog();
            \\      document.getElementById('createTableName').focus();
            \\    }
            \\
            \\    function openAmendMode(tableName) {
            \\      studioMode = 'amend';
            \\      currentStudioTable = tableName;
            \\      if (currentMainTab !== 'studio') {
            \\        switchMainTab('studio');
            \\      }
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableName.toLowerCase()) : null;
            \\      if (!tbl) {
            \\        currentStudioTable = null;
            \\        studioMode = null;
            \\        const emptyEl = document.getElementById('studioEmptyState');
            \\        const createEl = document.getElementById('studioCreatePanel');
            \\        const amendEl = document.getElementById('studioAmendPanel');
            \\        if (emptyEl) emptyEl.style.display = 'flex';
            \\        if (createEl) createEl.style.display = 'none';
            \\        if (amendEl) amendEl.style.display = 'none';
            \\        return;
            \\      }
            \\      const emptyEl = document.getElementById('studioEmptyState');
            \\      const createEl = document.getElementById('studioCreatePanel');
            \\      const amendEl = document.getElementById('studioAmendPanel');
            \\      if (emptyEl) emptyEl.style.display = 'none';
            \\      if (createEl) createEl.style.display = 'none';
            \\      if (amendEl) amendEl.style.display = 'block';
            \\
            \\      document.getElementById('amendTableTitle').innerText = tbl.name;
            \\      document.getElementById('amendTableRowCount').innerText = (tbl.row_count || 0) + ' rows';
            \\      document.getElementById('amendTableColCount').innerText = tbl.columns.length + ' columns';
            \\      document.getElementById('amendTableDesc').value = tbl.description || '';
            \\      document.getElementById('addColumnForm').style.display = 'none';
            \\
            \\      renderAmendColumnsTable(tbl);
            \\      renderStudioCatalog();
            \\    }
            \\
            \\    function cancelStudioEdit() {
            \\      document.getElementById('studioCreatePanel').style.display = 'none';
            \\      if (currentStudioTable) {
            \\        document.getElementById('studioAmendPanel').style.display = 'block';
            \\        studioMode = 'amend';
            \\      } else {
            \\        document.getElementById('studioEmptyState').style.display = 'flex';
            \\        studioMode = null;
            \\      }
            \\      renderStudioCatalog();
            \\    }
            \\
            \\    function renderStudioCatalog() {
            \\      const container = document.getElementById('studioCatalogList');
            \\      const countEl = document.getElementById('studioCatalogCount');
            \\      if (!container) return;
            \\      countEl.innerText = (knownTables ? knownTables.length : 0) + ' Tables';
            \\      const searchInput = document.getElementById('studioSearchInput');
            \\      const filter = (searchInput ? searchInput.value : '').toLowerCase().trim();
            \\
            \\      let filtered = knownTables || [];
            \\      if (filter) {
            \\        filtered = filtered.filter(t => t.name.toLowerCase().includes(filter) || (t.description && t.description.toLowerCase().includes(filter)));
            \\      }
            \\
            \\      if (filtered.length === 0) {
            \\        container.innerHTML = '<div style="font-size:12px;color:#64748b;padding:12px;text-align:center;">' + (filter ? 'No matching tables.' : 'No tables yet.') + '</div>';
            \\        return;
            \\      }
            \\
            \\      let html = '';
            \\      for (const t of filtered) {
            \\        const isAct = (currentStudioTable && currentStudioTable.toLowerCase() === t.name.toLowerCase()) ? ' active' : '';
            \\        html += '<div class="studio-catalog-item' + isAct + '" data-table="' + escapeHtml(t.name) + '" onclick="openAmendMode(this.dataset.table)">';
            \\        html += '<div class="catalog-item-header">';
            \\        html += '<span class="catalog-item-name">&bull; ' + escapeHtml(t.name) + '</span>';
            \\        html += '<div class="catalog-item-badges">';
            \\        html += '<span class="badge-type">' + t.columns.length + ' col</span>';
            \\        html += '<span class="badge-type">' + (t.row_count || 0) + ' r</span>';
            \\        html += '</div></div>';
            \\        const desc = t.description ? escapeHtml(t.description) : '<em style="color:#64748b">No description set</em>';
            \\        html += '<div class="catalog-item-desc">' + desc + '</div>';
            \\        html += '</div>';
            \\      }
            \\      container.innerHTML = html;
            \\    }
            \\
            \\    function filterStudioCatalog() {
            \\      renderStudioCatalog();
            \\    }
            \\
            \\    function renderCreateColumnsTable() {
            \\      const tbody = document.getElementById('createColumnsTableBody');
            \\      let html = '';
            \\      const types = ['INTEGER', 'REAL', 'TEXT', 'BOOLEAN', 'TIMESTAMP', 'BLOB'];
            \\      createColumns.forEach((col, idx) => {
            \\        const isInt = col.type === 'INTEGER';
            \\        html += '<tr>';
            \\        html += '<td><input type="text" class="form-control" style="font-family:monospace;font-size:12px;" value="' + escapeHtml(col.name) + '" placeholder="column_name" oninput="onCreateColNameChange(' + idx + ', this.value)"></td>';
            \\        html += '<td><select class="form-control" style="font-size:12px;" onchange="onCreateColTypeChange(' + idx + ', this.value)">';
            \\        for (const t of types) {
            \\          html += '<option value="' + t + '"' + (col.type === t ? ' selected' : '') + '>' + t + '</option>';
            \\        }
            \\        html += '</select></td>';
            \\        html += '<td style="text-align:center;"><input type="radio" name="createPkRadio" ' + (col.pk ? 'checked' : '') + ' onchange="setCreatePk(' + idx + ')"></td>';
            \\        html += '<td style="text-align:center;"><input type="checkbox" ' + (isInt ? '' : 'disabled ') + (col.auto_increment ? 'checked' : '') + ' title="' + (isInt ? 'Auto-increment sequence' : 'Only INTEGER columns support auto-increment') + '" onchange="onCreateColAutoIncChange(' + idx + ', this.checked)"></td>';
            \\        html += '<td style="text-align:center;"><input type="checkbox" ' + (col.nullable ? 'checked' : '') + ' ' + (col.pk ? 'disabled' : '') + ' onchange="onCreateColNullableChange(' + idx + ', this.checked)"></td>';
            \\        html += '<td><input type="text" class="form-control" style="font-size:12px;" value="' + escapeHtml(col.desc) + '" placeholder="Field description..." oninput="onCreateColDescChange(' + idx + ', this.value)"></td>';
            \\        html += '<td style="text-align:center;">';
            \\        if (createColumns.length > 1) {
            \\          html += '<button type="button" class="btn-danger btn-xs" onclick="removeCreateColumnRow(' + idx + ')">&times;</button>';
            \\        } else {
            \\          html += '<span style="color:#64748b">-</span>';
            \\        }
            \\        html += '</td>';
            \\        html += '</tr>';
            \\      });
            \\      tbody.innerHTML = html;
            \\    }
            \\
            \\    function onCreateColNameChange(idx, val) {
            \\      createColumns[idx].name = val;
            \\      updateDdlPreview();
            \\    }
            \\    function onCreateColTypeChange(idx, val) {
            \\      createColumns[idx].type = val;
            \\      if (val !== 'INTEGER') {
            \\        createColumns[idx].auto_increment = false;
            \\      }
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\    }
            \\    function onCreateColAutoIncChange(idx, checked) {
            \\      createColumns[idx].auto_increment = checked;
            \\      updateDdlPreview();
            \\    }
            \\    function onCreateColNullableChange(idx, checked) {
            \\      createColumns[idx].nullable = checked;
            \\      updateDdlPreview();
            \\    }
            \\    function onCreateColDescChange(idx, val) {
            \\      createColumns[idx].desc = val;
            \\    }
            \\    function setCreatePk(idx) {
            \\      for (let i = 0; i < createColumns.length; i++) {
            \\        if (i === idx) {
            \\          createColumns[i].pk = true;
            \\          createColumns[i].nullable = false;
            \\          if (createColumns[i].type === 'INTEGER') {
            \\            createColumns[i].auto_increment = true;
            \\          }
            \\        } else {
            \\          createColumns[i].pk = false;
            \\        }
            \\      }
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\    }
            \\    function addCreateColumnRow() {
            \\      createColumns.push({ name: '', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: '' });
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\    }
            \\    function removeCreateColumnRow(idx) {
            \\      if (createColumns.length <= 1) return;
            \\      const wasPk = createColumns[idx].pk;
            \\      createColumns.splice(idx, 1);
            \\      if (wasPk && createColumns.length > 0) {
            \\        createColumns[0].pk = true;
            \\        createColumns[0].nullable = false;
            \\        if (createColumns[0].type === 'INTEGER') {
            \\          createColumns[0].auto_increment = true;
            \\        }
            \\      }
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\    }
            \\
            \\    function updateDdlPreview() {
            \\      const name = document.getElementById('createTableName').value.trim() || 'table_name';
            \\      let ddl = 'CREATE TABLE ' + name + ' (\n';
            \\      const parts = [];
            \\      for (const c of createColumns) {
            \\        const cname = c.name.trim() || 'column';
            \\        let line = '  ' + cname + ' ' + c.type;
            \\        if (c.pk) {
            \\          line += ' PRIMARY KEY';
            \\          if (c.auto_increment) {
            \\            line += ' AUTO_INCREMENT';
            \\          }
            \\        } else {
            \\          if (c.auto_increment) {
            \\            line += ' AUTO_INCREMENT';
            \\          }
            \\          if (!c.nullable) {
            \\            line += ' NOT NULL';
            \\          }
            \\        }
            \\        parts.push(line);
            \\      }
            \\      ddl += parts.join(',\n') + '\n);';
            \\      document.getElementById('createDdlPreview').innerText = ddl;
            \\    }
            \\
            \\    function applyPreset(preset) {
            \\      if (preset === 'users') {
            \\        document.getElementById('createTableName').value = 'users';
            \\        document.getElementById('createTableDesc').value = 'User accounts with authentication, profile information, and audit dates.';
            \\        createColumns = [
            \\          { name: 'id', type: 'INTEGER', pk: true, auto_increment: true, nullable: false, desc: 'Unique account identifier' },
            \\          { name: 'username', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: 'Login username' },
            \\          { name: 'email', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: 'User email address' },
            \\          { name: 'age', type: 'INTEGER', pk: false, auto_increment: false, nullable: true, desc: 'User age in years' },
            \\          { name: 'created_at', type: 'TIMESTAMP', pk: false, auto_increment: false, nullable: true, desc: 'Account registration timestamp' }
            \\        ];
            \\      } else if (preset === 'products') {
            \\        document.getElementById('createTableName').value = 'products';
            \\        document.getElementById('createTableDesc').value = 'Product inventory catalog tracking SKUs, unit pricing, and stock status.';
            \\        createColumns = [
            \\          { name: 'sku', type: 'TEXT', pk: true, auto_increment: false, nullable: false, desc: 'Stock keeping unit alphanumeric code' },
            \\          { name: 'name', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: 'Product title / name' },
            \\          { name: 'price', type: 'REAL', pk: false, auto_increment: false, nullable: true, desc: 'Unit price in USD' },
            \\          { name: 'in_stock', type: 'BOOLEAN', pk: false, auto_increment: false, nullable: true, desc: 'Item availability flag' },
            \\          { name: 'updated_at', type: 'TIMESTAMP', pk: false, auto_increment: false, nullable: true, desc: 'Last inventory update timestamp' }
            \\        ];
            \\      } else if (preset === 'orders') {
            \\        document.getElementById('createTableName').value = 'orders';
            \\        document.getElementById('createTableDesc').value = 'Customer orders and billing transaction records.';
            \\        createColumns = [
            \\          { name: 'order_id', type: 'INTEGER', pk: true, auto_increment: true, nullable: false, desc: 'Sequential order reference ID' },
            \\          { name: 'customer_id', type: 'INTEGER', pk: false, auto_increment: false, nullable: true, desc: 'Reference to customer account ID' },
            \\          { name: 'total_cents', type: 'INTEGER', pk: false, auto_increment: false, nullable: true, desc: 'Total transaction amount in cents' },
            \\          { name: 'status', type: 'TEXT', pk: false, auto_increment: false, nullable: true, desc: 'Order status (pending, completed, canceled)' },
            \\          { name: 'placed_at', type: 'TIMESTAMP', pk: false, auto_increment: false, nullable: true, desc: 'Order placement timestamp' }
            \\        ];
            \\      } else if (preset === 'metrics') {
            \\        document.getElementById('createTableName').value = 'sensor_metrics';
            \\        document.getElementById('createTableDesc').value = 'Edge device telemetry capturing temperature, battery voltage, and logs.';
            \\        createColumns = [
            \\          { name: 'device_id', type: 'TEXT', pk: true, auto_increment: false, nullable: false, desc: 'Hardware MAC or serial identifier' },
            \\          { name: 'temperature', type: 'REAL', pk: false, auto_increment: false, nullable: true, desc: 'Ambient reading in Celsius' },
            \\          { name: 'voltage', type: 'REAL', pk: false, auto_increment: false, nullable: true, desc: 'Battery voltage level' },
            \\          { name: 'recorded_at', type: 'TIMESTAMP', pk: false, auto_increment: false, nullable: true, desc: 'Telemetry reading timestamp' }
            \\        ];
            \\      }
            \\      renderCreateColumnsTable();
            \\      updateDdlPreview();
            \\    }
            \\
            \\    async function submitCreateTable() {
            \\      const name = document.getElementById('createTableName').value.trim();
            \\      if (!name) {
            \\        showStudioAlert('error', 'Please provide a table name.');
            \\        return;
            \\      }
            \\      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
            \\        showStudioAlert('error', 'Table name must start with a letter/underscore and contain only alphanumeric characters or underscores.');
            \\        return;
            \\      }
            \\
            \\      const colNames = new Set();
            \\      let hasPk = false;
            \\
            \\      for (const col of createColumns) {
            \\        const cname = col.name.trim();
            \\        if (!cname) {
            \\          showStudioAlert('error', 'All columns must have a name.');
            \\          return;
            \\        }
            \\        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(cname)) {
            \\          showStudioAlert('error', 'Column name "' + cname + '" contains invalid characters.');
            \\          return;
            \\        }
            \\        if (colNames.has(cname.toLowerCase())) {
            \\          showStudioAlert('error', 'Duplicate column name "' + cname + '".');
            \\          return;
            \\        }
            \\        colNames.add(cname.toLowerCase());
            \\        if (col.pk) hasPk = true;
            \\      }
            \\
            \\      if (!hasPk) {
            \\        showStudioAlert('error', 'Table must have exactly one PRIMARY KEY column.');
            \\        return;
            \\      }
            \\
            \\      const payload = {
            \\        name: name,
            \\        description: document.getElementById('createTableDesc').value.trim(),
            \\        columns: createColumns.map(c => ({
            \\          name: c.name.trim(),
            \\          type: c.type,
            \\          primary_key: !!c.pk,
            \\          auto_increment: !!c.auto_increment,
            \\          nullable: !c.pk && !!c.nullable,
            \\          description: c.desc.trim()
            \\        }))
            \\      };
            \\
            \\      try {
            \\        const res = await fetch('/api/tables/create', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify(payload)
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Table created successfully!');
            \\        await fetchTables();
            \\        openAmendMode(name);
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function renderAmendColumnsTable(tbl) {
            \\      const tbody = document.getElementById('amendColumnsTableBody');
            \\      let html = '';
            \\      const types = ['INTEGER', 'REAL', 'TEXT', 'BOOLEAN', 'TIMESTAMP', 'BLOB'];
            \\      for (const col of tbl.columns) {
            \\        html += '<tr>';
            \\        html += '<td><div style="display:flex;align-items:center;gap:6px;"><strong style="font-family:monospace;color:#f8fafc;">' + escapeHtml(col.name) + '</strong><button class="pill pill-alter" style="padding:1px 5px;font-size:10px;" data-col="' + escapeHtml(col.name) + '" onclick="promptRenameColumn(this.dataset.col)" title="Rename column">✏️</button></div></td>';
            \\        html += '<td><select class="form-control" style="font-size:12px;padding:3px 6px;" data-col="' + escapeHtml(col.name) + '" onchange="submitModifyColumn(this.dataset.col, this.value)">';
            \\        for (const t of types) {
            \\          html += '<option value="' + t + '"' + (col.type === t ? ' selected' : '') + '>' + t + '</option>';
            \\        }
            \\        html += '</select></td>';
            \\        html += '<td style="text-align:center;">';
            \\        if (col.primary_key) {
            \\          html += '<div style="display:inline-flex;align-items:center;gap:4px;"><span class="pk-badge">PK</span><button class="btn-secondary btn-xs" style="padding:1px 5px;font-size:10px;line-height:1;" onclick="submitDropPrimaryKey()" title="Drop primary key">✕</button></div>';
            \\        } else {
            \\          html += '<button class="btn-secondary btn-xs" style="color:#f59e0b;border-color:rgba(245,158,11,0.4);padding:2px 6px;font-size:11px;" data-col="' + escapeHtml(col.name) + '" onclick="submitSetPrimaryKey(this.dataset.col)" title="Make this column the primary key">Make PK</button>';
            \\        }
            \\        html += '</td>';
            \\        html += '<td style="text-align:center;">' + (col.auto_increment ? '<span class="badge-ai">AUTO</span>' : '<span style="color:#64748b">-</span>') + '</td>';
            \\        html += '<td style="text-align:center;font-size:11px;color:' + (col.nullable ? '#94a3b8' : '#f59e0b;font-weight:700') + '">' + (col.nullable ? 'YES' : 'NO') + '</td>';
            \\        html += '<td><div style="display:flex;gap:4px;"><input type="text" id="desc_' + escapeHtml(col.name) + '" class="form-control" style="font-size:12px;padding:4px 8px;" value="' + escapeHtml(col.description || '') + '" placeholder="Field description..."><button class="btn-primary btn-xs" data-col="' + escapeHtml(col.name) + '" onclick="submitSaveColDescription(this.dataset.col)">Save</button></div></td>';
            \\        html += '<td style="text-align:center;">';
            \\        html += '<button class="btn-danger btn-xs" data-col="' + escapeHtml(col.name) + '" data-pk="' + (col.primary_key ? '1' : '0') + '" onclick="submitDropColumn(this.dataset.col, this.dataset.pk === \'1\')" title="Drop column">Drop</button>';
            \\        html += '</td>';
            \\        html += '</tr>';
            \\      }
            \\      tbody.innerHTML = html;
            \\    }
            \\
            \\    async function submitSaveTableDescription() {
            \\      if (!currentStudioTable) return;
            \\      const desc = document.getElementById('amendTableDesc').value.trim();
            \\      try {
            \\        const res = await fetch('/api/tables/save_metadata', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            description: desc
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Table description updated successfully.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitSaveColDescription(colName) {
            \\      if (!currentStudioTable) return;
            \\      const inputEl = document.getElementById('desc_' + colName);
            \\      if (!inputEl) return;
            \\      const desc = inputEl.value.trim();
            \\      try {
            \\        const res = await fetch('/api/tables/save_metadata', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            columns: [{ name: colName, description: desc }]
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', 'Description for column "' + colName + '" saved.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitModifyColumn(colName, newType) {
            \\      if (!currentStudioTable) return;
            \\      try {
            \\        const res = await fetch('/api/tables/modify_column', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            column: { name: colName, type: newType }
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          await fetchTables();
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || ('Column "' + colName + '" modified to ' + newType));
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function promptRenameColumn(oldName) {
            \\      if (!currentStudioTable) return;
            \\      const newName = prompt("Enter new name for column '" + oldName + "':", oldName);
            \\      if (!newName || newName.trim() === '' || newName.trim() === oldName) return;
            \\      const trimmed = newName.trim();
            \\      try {
            \\        const res = await fetch('/api/tables/rename_column', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            old_name: oldName,
            \\            new_name: trimmed
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Column renamed successfully.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitDropColumn(colName, isPk) {
            \\      if (!currentStudioTable) return;
            \\      const warnMsg = isPk
            \\        ? "Column '" + colName + "' is the PRIMARY KEY of table '" + currentStudioTable + "'. Dropping it will drop the primary key and delete all data in this column. Are you sure?"
            \\        : "Are you sure you want to drop column '" + colName + "' from table '" + currentStudioTable + "'? This will permanently delete its data.";
            \\      if (!confirm(warnMsg)) {
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/tables/drop_column', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            column: colName
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Column dropped successfully.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitSetPrimaryKey(colName) {
            \\      if (!currentStudioTable) return;
            \\      if (!confirm("Set primary key of table '" + currentStudioTable + "' to '" + colName + "'? The existing primary key will be dropped and recreated from metadata.")) {
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/tables/set_primary_key', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            column: colName
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Primary key updated successfully.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitDropPrimaryKey() {
            \\      if (!currentStudioTable) return;
            \\      if (!confirm("Are you sure you want to drop the primary key from table '" + currentStudioTable + "'?")) {
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/tables/drop_primary_key', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Primary key dropped successfully.');
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function toggleAddColumnForm() {
            \\      const el = document.getElementById('addColumnForm');
            \\      if (el.style.display === 'none' || !el.style.display) {
            \\        el.style.display = 'block';
            \\        document.getElementById('newColName').value = '';
            \\        document.getElementById('newColType').value = 'TEXT';
            \\        document.getElementById('newColPk').checked = false;
            \\        document.getElementById('newColAutoInc').checked = false;
            \\        document.getElementById('newColAutoInc').disabled = true;
            \\        document.getElementById('newColDesc').value = '';
            \\        document.getElementById('newColName').focus();
            \\      } else {
            \\        el.style.display = 'none';
            \\      }
            \\    }
            \\
            \\    function onNewColTypeChange() {
            \\      const t = document.getElementById('newColType').value;
            \\      const ai = document.getElementById('newColAutoInc');
            \\      if (t === 'INTEGER') {
            \\        ai.disabled = false;
            \\      } else {
            \\        ai.checked = false;
            \\        ai.disabled = true;
            \\      }
            \\    }
            \\
            \\    async function submitAddColumn() {
            \\      if (!currentStudioTable) return;
            \\      const nameInput = document.getElementById('newColName');
            \\      const typeSelect = document.getElementById('newColType');
            \\      const pkCheckbox = document.getElementById('newColPk');
            \\      const autoIncCheckbox = document.getElementById('newColAutoInc');
            \\      const descInput = document.getElementById('newColDesc');
            \\      const name = nameInput.value.trim();
            \\      if (!name) {
            \\        showStudioAlert('error', 'Please provide a column name.');
            \\        return;
            \\        }
            \\      const type = typeSelect.value;
            \\      const isPk = pkCheckbox ? pkCheckbox.checked : false;
            \\      const isAutoInc = (type === 'INTEGER') && autoIncCheckbox.checked;
            \\      const desc = descInput.value.trim();
            \\      try {
            \\        const res = await fetch('/api/tables/add_column', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: currentStudioTable,
            \\            column: {
            \\              name: name,
            \\              type: type,
            \\              primary_key: isPk,
            \\              auto_increment: isAutoInc,
            \\              nullable: !isPk,
            \\              description: desc
            \\            }
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Column added successfully.');
            \\        nameInput.value = '';
            \\        descInput.value = '';
            \\        if (pkCheckbox) pkCheckbox.checked = false;
            \\        autoIncCheckbox.checked = false;
            \\        autoIncCheckbox.disabled = true;
            \\        document.getElementById('addColumnForm').style.display = 'none';
            \\        await fetchTables();
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function submitDropCurrentTable() {
            \\      if (!currentStudioTable) return;
            \\      if (!confirm("Are you sure you want to drop table '" + currentStudioTable + "'? All rows and metadata will be permanently deleted across the cluster.")) {
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/tables/drop', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({ table: currentStudioTable })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showStudioAlert('error', data.error);
            \\          return;
            \\        }
            \\        showStudioAlert('success', data.message || 'Table dropped successfully.');
            \\        currentStudioTable = null;
            \\        studioMode = null;
            \\        await fetchTables();
            \\        renderStudioCatalog();
            \\        document.getElementById('studioAmendPanel').style.display = 'none';
            \\        document.getElementById('studioCreatePanel').style.display = 'none';
            \\        document.getElementById('studioEmptyState').style.display = 'flex';
            \\      } catch (e) {
            \\        showStudioAlert('error', 'Network error: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function queryTableInConsole() {
            \\      if (!currentStudioTable) return;
            \\      switchMainTab('sql');
            \\      insertSnippet('SELECT * FROM ' + currentStudioTable + ';');
            \\      runQuery();
            \\    }
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
            \\        renderStudioCatalog();
            \\        updateTableDataTableSelect();
            \\        if (currentStudioTable) {
            \\          const cur = knownTables.find(t => t.name.toLowerCase() === currentStudioTable.toLowerCase());
            \\          if (cur) {
            \\            document.getElementById('amendTableRowCount').innerText = (cur.row_count || 0) + ' rows';
            \\            document.getElementById('amendTableColCount').innerText = cur.columns.length + ' columns';
            \\            if (document.activeElement && document.activeElement.id && document.activeElement.id.startsWith('desc_')) {
            \\              // User actively editing a column description input; don't re-render rows to avoid stealing focus
            \\            } else {
            \\              renderAmendColumnsTable(cur);
            \\            }
            \\          }
            \\        }
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
            \\        html += '<div class="schema-table-name">';
            \\        html += '<span data-snippet="SELECT * FROM ' + escapeHtml(tbl.name) + ';" onclick="insertSnippet(this.dataset.snippet)">&bull; ' + escapeHtml(tbl.name) + '</span>';
            \\        html += '<div style="display:flex;align-items:center;gap:6px;">';
            \\        html += '<span style="font-size:10px;color:#94a3b8">' + tbl.columns.length + ' cols</span>';
            \\        html += '<button class="pill pill-alter" style="padding:1px 6px;font-size:10px;" data-table="' + escapeHtml(tbl.name) + '" onclick="openAmendMode(this.dataset.table)">✏️ Amend</button>';
            \\        html += '</div>';
            \\        html += '</div>';
            \\        if (tbl.description) {
            \\          html += '<div style="font-size:11px;color:#94a3b8;margin-bottom:6px;font-style:italic;line-height:1.3;">' + escapeHtml(tbl.description) + '</div>';
            \\        }
            \\        html += '<div class="schema-col-list">';
            \\        for (const col of tbl.columns) {
            \\          const colTooltip = col.description ? (' title="' + escapeHtml(col.description) + '"') : '';
            \\          html += '<div class="schema-col-item"' + colTooltip + ' data-snippet="ALTER TABLE ' + escapeHtml(tbl.name) + ' MODIFY COLUMN ' + escapeHtml(col.name) + ' ' + col.type + ';" onclick="insertSnippet(this.dataset.snippet)">';
            \\          html += '<span>' + escapeHtml(col.name) + (col.primary_key ? '<span class="pk-badge">PK</span>' : '') + (col.auto_increment ? '<span class="badge-ai">AI</span>' : '') + '</span>';
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
            \\          candidates.push({ label: t.name, type: 'tab', desc: t.description || ('Table (' + t.columns.length + ' cols)') });
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
            \\            const desc = (col.description ? col.description + ' &bull; ' : '') + col.type + (col.primary_key ? ' (PK)' : '') + ' in ' + targetTbl.name;
            \\            candidates.push({ label: col.name, type: 'col', desc: desc });
            \\          }
            \\        } else {
            \\          for (const t of knownTables) {
            \\            for (const col of t.columns) {
            \\              const desc = (col.description ? col.description + ' &bull; ' : '') + col.type + ' in ' + t.name;
            \\              candidates.push({ label: col.name, type: 'col', desc: desc });
            \\            }
            \\          }
            \\        }
            \\      } else if ((thirdLast === 'MODIFY' && secondLast === 'COLUMN') || (thirdLast === 'ADD' && secondLast === 'COLUMN')) {
            \\        candidates = candidates.concat(SQL_TYPES);
            \\      } else if (lastToken === 'FROM' || lastToken === 'INTO' || lastToken === 'UPDATE' || lastToken === 'DESCRIBE') {
            \\        for (const t of knownTables) {
            \\          candidates.push({ label: t.name, type: 'tab', desc: t.description || 'Table' });
            \\        }
            \\      } else if (lastToken === 'WHERE' || lastToken === 'SET' || lastToken === 'SELECT') {
            \\        for (const t of knownTables) {
            \\          for (const col of t.columns) {
            \\            const desc = (col.description ? col.description + ' &bull; ' : '') + col.type + ' in ' + t.name;
            \\            candidates.push({ label: col.name, type: 'col', desc: desc });
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
            \\        candidates.push({ label: t.name, type: 'tab', desc: t.description || 'Table' });
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
            \\              html += '<td>' + (val === null ? '<em style="color:#64748b">NULL</em>' : val) + '</td>';
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
            \\    // ==========================================
            \\    // Table Editor Table Display & Data Maintenance
            \\    // ==========================================
            \\    function showTableDataSelectionAlert(type, message) {
            \\      const el = document.getElementById('tableDataSelectionAlert');
            \\      if (!el) return;
            \\      el.className = 'alert-box ' + (type === 'error' ? 'alert-error' : 'alert-success');
            \\      el.innerText = message;
            \\      el.style.display = 'block';
            \\      setTimeout(() => {
            \\        if (el.innerText === message) el.style.display = 'none';
            \\      }, 5000);
            \\    }
            \\
            \\    function showGridAlert(type, message) {
            \\      const el = document.getElementById('tableDataGridAlert');
            \\      if (!el) return;
            \\      el.className = 'alert-box ' + (type === 'error' ? 'alert-error' : 'alert-success');
            \\      el.innerText = message;
            \\      el.style.display = 'block';
            \\      setTimeout(() => {
            \\        if (el.innerText === message) el.style.display = 'none';
            \\      }, 5000);
            \\    }
            \\
            \\    function updateTableDataTableSelect(targetTable) {
            \\      const select = document.getElementById('tableDataTableSelect');
            \\      if (!select) return;
            \\      if (!knownTables || knownTables.length === 0) {
            \\        select.innerHTML = '<option value="">No tables available</option>';
            \\        const metaEl = document.getElementById('tableDataTableMetadataInfo');
            \\        if (metaEl) metaEl.innerHTML = '';
            \\        const badge = document.getElementById('tableDataTableSummaryBadge');
            \\        if (badge) badge.style.display = 'none';
            \\        const body = document.getElementById('tableDataCriteriaBody');
            \\        if (body) body.innerHTML = '<tr><td colspan="8" style="text-align: center; color: #94a3b8; padding: 30px;">No tables found in StrataDB cluster. Create one in Table Studio.</td></tr>';
            \\        tableDataTable = null;
            \\        return;
            \\      }
            \\      const prevVal = targetTable || select.value || tableDataTable || knownTables[0].name;
            \\      let html = '';
            \\      for (const t of knownTables) {
            \\        const desc = t.description ? (' - ' + escapeHtml(t.description)) : '';
            \\        const sel = (t.name.toLowerCase() === prevVal.toLowerCase()) ? ' selected' : '';
            \\        html += '<option value="' + escapeHtml(t.name) + '"' + sel + '>' + escapeHtml(t.name) + desc + '</option>';
            \\      }
            \\      select.innerHTML = html;
            \\      const chosen = targetTable || (select.value ? select.value : knownTables[0].name);
            \\      const hasRenderedRows = document.getElementById('tableDataCriteriaBody') && document.getElementById('tableDataCriteriaBody').querySelector('tr[data-rendered]');
            \\      if (chosen && (!tableDataTable || targetTable || !hasRenderedRows)) {
            \\        onTableDataTableChange(chosen);
            \\      }
            \\    }
            \\
            \\    function onTableDataTableChange(tableName) {
            \\      if (!tableName) return;
            \\      tableDataTable = tableName;
            \\      const select = document.getElementById('tableDataTableSelect');
            \\      if (select && select.value !== tableName) select.value = tableName;
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableName.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\
            \\      const badge = document.getElementById('tableDataTableSummaryBadge');
            \\      if (badge) {
            \\        badge.innerText = (tbl.row_count || 0) + ' rows';
            \\        badge.style.display = 'inline-block';
            \\      }
            \\
            \\      const metaEl = document.getElementById('tableDataTableMetadataInfo');
            \\      if (metaEl) {
            \\        const pks = tbl.columns.filter(c => c.primary_key).map(c => c.name);
            \\        metaEl.innerHTML = '<strong>' + (tbl.row_count || 0) + '</strong> records &bull; ' +
            \\          '<strong>' + tbl.columns.length + '</strong> columns &bull; ' +
            \\          'PK: <span style="color:#f59e0b;font-family:monospace;">' + (pks.length ? pks.join(', ') : 'None') + '</span>' +
            \\          (tbl.description ? (' &bull; <span style="font-style:italic;">' + escapeHtml(tbl.description) + '</span>') : '');
            \\      }
            \\      renderTableDataCriteria(tbl);
            \\    }
            \\
            \\    function renderTableDataCriteria(tbl) {
            \\      const tbody = document.getElementById('tableDataCriteriaBody');
            \\      if (!tbody) return;
            \\      if (!tbl || !tbl.columns || tbl.columns.length === 0) {
            \\        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: #94a3b8; padding: 20px;">No columns in table.</td></tr>';
            \\        return;
            \\      }
            \\      let html = '';
            \\      const operators = ['=', '<>', '>', '>=', '<', '<=', 'LIKE', 'BETWEEN', 'IS NULL', 'IS NOT NULL'];
            \\      for (let i = 0; i < tbl.columns.length; i++) {
            \\        const col = tbl.columns[i];
            \\        const isPk = !!col.primary_key;
            \\        html += '<tr data-rendered="1">';
            \\        html += '<td style="text-align:center;"><input type="checkbox" id="table_data_out_' + escapeHtml(col.name) + '" checked title="Output ' + escapeHtml(col.name) + '"></td>';
            \\        html += '<td style="text-align:center;">' + (isPk ? '<span class="table-data-key-badge" title="Primary Key">🔑</span>' : '<span style="color:#475569">-</span>') + '</td>';
            \\        html += '<td><strong style="font-family:monospace;color:#f8fafc;">' + escapeHtml(col.name) + '</strong></td>';
            \\        html += '<td><span style="color:#cbd5e1;font-size:12px;">' + escapeHtml(col.description || '-') + '</span></td>';
            \\        html += '<td><span class="badge-type">' + escapeHtml(col.type) + '</span></td>';
            \\        html += '<td><select id="table_data_op_' + escapeHtml(col.name) + '" class="table-data-op-select" onchange="onTableDataOperatorChange(\'' + escapeHtml(col.name) + '\', this.value)">';
            \\        for (const op of operators) {
            \\          html += '<option value="' + op + '">' + op + '</option>';
            \\        }
            \\        html += '</select></td>';
            \\        html += '<td><input type="text" id="table_data_low_' + escapeHtml(col.name) + '" class="table-data-val-input" placeholder="Value low..."></td>';
            \\        html += '<td><input type="text" id="table_data_high_' + escapeHtml(col.name) + '" class="table-data-val-input" placeholder="Value high" style="display:none;"></td>';
            \\        html += '</tr>';
            \\      }
            \\      tbody.innerHTML = html;
            \\    }
            \\
            \\    function onTableDataOperatorChange(colName, op) {
            \\      const lowInp = document.getElementById('table_data_low_' + colName);
            \\      const highInp = document.getElementById('table_data_high_' + colName);
            \\      if (!lowInp || !highInp) return;
            \\      if (op === 'IS NULL' || op === 'IS NOT NULL') {
            \\        lowInp.style.display = 'none';
            \\        highInp.style.display = 'none';
            \\      } else if (op === 'BETWEEN') {
            \\        lowInp.style.display = 'block';
            \\        highInp.style.display = 'block';
            \\      } else {
            \\        lowInp.style.display = 'block';
            \\        highInp.style.display = 'none';
            \\      }
            \\    }
            \\
            \\    function resetTableDataCriteria() {
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (tbl) renderTableDataCriteria(tbl);
            \\    }
            \\
            \\    function toggleAllCriteriaDisplay(checked) {
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\      for (const col of tbl.columns) {
            \\        const el = document.getElementById('table_data_out_' + col.name);
            \\        if (el) el.checked = checked;
            \\      }
            \\    }
            \\
            \\    function setTableDataMaxHits(num) {
            \\      const input = document.getElementById('tableDataMaxHits');
            \\      if (input) input.value = num;
            \\      const chips = document.querySelectorAll('.table-data-chips .table-data-chip');
            \\      chips.forEach(chip => {
            \\        const chipVal = chip.innerText.trim() === 'All' ? 0 : parseInt(chip.innerText);
            \\        if (chipVal === num) chip.classList.add('active');
            \\        else chip.classList.remove('active');
            \\      });
            \\    }
            \\
            \\    function collectTableDataCriteria() {
            \\      if (!tableDataTable) return null;
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return null;
            \\      const outputCols = [];
            \\      const criteria = [];
            \\      for (const col of tbl.columns) {
            \\        const outBox = document.getElementById('table_data_out_' + col.name);
            \\        if (outBox && outBox.checked) {
            \\          outputCols.push(col.name);
            \\        }
            \\        const opSel = document.getElementById('table_data_op_' + col.name);
            \\        const lowInp = document.getElementById('table_data_low_' + col.name);
            \\        const highInp = document.getElementById('table_data_high_' + col.name);
            \\        if (opSel) {
            \\          const op = opSel.value;
            \\          const low = lowInp ? lowInp.value.trim() : '';
            \\          const high = highInp ? highInp.value.trim() : '';
            \\          if (op === 'IS NULL' || op === 'IS NOT NULL' || low !== '') {
            \\            criteria.push({
            \\              column: col.name,
            \\              op: op,
            \\              low: low,
            \\              high: high
            \\            });
            \\          }
            \\        }
            \\      }
            \\      const maxHitsInput = document.getElementById('tableDataMaxHits');
            \\      const maxHitsVal = maxHitsInput ? parseInt(maxHitsInput.value) : 500;
            \\      return {
            \\        table: tableDataTable,
            \\        columns: outputCols,
            \\        filters: criteria,
            \\        max_rows: isNaN(maxHitsVal) ? 500 : maxHitsVal
            \\      };
            \\    }
            \\
            \\    async function countTableDataEntries() {
            \\      const req = collectTableDataCriteria();
            \\      if (!req) {
            \\        showTableDataSelectionAlert('error', 'Please select a table.');
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/table_data/count', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: req.table,
            \\            filters: req.filters
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showTableDataSelectionAlert('error', 'Error counting entries: ' + data.error);
            \\          return;
            \\        }
            \\        document.getElementById('tableDataCountModalValue').innerText = data.count;
            \\        document.getElementById('tableDataCountModalTable').innerText = 'Table: ' + req.table + ' (' + data.count + ' matching rows)';
            \\        document.getElementById('tableDataCountModal').style.display = 'flex';
            \\      } catch (e) {
            \\        showTableDataSelectionAlert('error', 'Network error counting entries: ' + e.message);
            \\      }
            \\    }
            \\
            \\    async function executeTableDataQuery() {
            \\      const req = collectTableDataCriteria();
            \\      if (!req) {
            \\        showTableDataSelectionAlert('error', 'Please select a table.');
            \\        return;
            \\      }
            \\      if (req.columns.length === 0) {
            \\        showTableDataSelectionAlert('error', 'Please select at least one column for output display.');
            \\        return;
            \\      }
            \\      try {
            \\        const res = await fetch('/api/table_data/query', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify(req)
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showTableDataSelectionAlert('error', 'Query execution failed: ' + data.error);
            \\          return;
            \\        }
            \\        tableDataResultData = data;
            \\        tableDataSelectedRows.clear();
            \\        tableDataSortCol = null;
            \\        tableDataSortAsc = true;
            \\        const filterInp = document.getElementById('tableDataGridFilterInput');
            \\        if (filterInp) filterInp.value = '';
            \\        document.getElementById('tableDataSelectionScreen').style.display = 'none';
            \\        document.getElementById('tableDataGridScreen').style.display = 'block';
            \\        renderTableDataGrid();
            \\      } catch (e) {
            \\        showTableDataSelectionAlert('error', 'Network error executing TableData query: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function backToTableDataSelection() {
            \\      document.getElementById('tableDataGridScreen').style.display = 'none';
            \\      document.getElementById('tableDataSelectionScreen').style.display = 'block';
            \\    }
            \\
            \\    function refreshTableDataGrid() {
            \\      executeTableDataQuery();
            \\    }
            \\
            \\    function formatColHeader(col) {
            \\      if (tableDataHeaderFormat === 'tech') return col.name;
            \\      if (tableDataHeaderFormat === 'text') return col.description || col.name;
            \\      return col.name + (col.description ? ' (' + col.description + ')' : '');
            \\    }
            \\
            \\    function renderTableDataGrid() {
            \\      if (!tableDataResultData) return;
            \\      document.getElementById('tableDataGridTableTitle').innerText = tableDataResultData.table;
            \\      const returned = tableDataResultData.returned_rows || (tableDataResultData.rows ? tableDataResultData.rows.length : 0);
            \\      document.getElementById('tableDataGridHitsBadge').innerText = returned + ' / ' + tableDataResultData.total_hits + ' entries';
            \\
            \\      let thHtml = '<tr>';
            \\      thHtml += '<th style="width: 38px; text-align: center;"><input type="checkbox" id="tableDataSelectAllCheckbox" onclick="toggleSelectAllTableDataRows(this.checked)"></th>';
            \\      thHtml += '<th style="width: 48px; text-align: center; color: #94a3b8;">#</th>';
            \\      for (let c = 0; c < tableDataResultData.columns.length; c++) {
            \\        const col = tableDataResultData.columns[c];
            \\        const isSorted = tableDataSortCol === col.name;
            \\        const sortIcon = isSorted ? (tableDataSortAsc ? ' ▲' : ' ▼') : '';
            \\        const pkIcon = col.primary_key ? ' 🔑' : '';
            \\        thHtml += '<th style="cursor: pointer; user-select: none;" onclick="sortTableDataGridByCol(\'' + escapeHtml(col.name) + '\')">' +
            \\                  escapeHtml(formatColHeader(col)) + pkIcon + '<span style="color: var(--accent);">' + sortIcon + '</span></th>';
            \\      }
            \\      thHtml += '</tr>';
            \\      document.getElementById('tableDataGridThead').innerHTML = thHtml;
            \\
            \\      const filterInput = document.getElementById('tableDataGridFilterInput');
            \\      const filterText = (filterInput ? filterInput.value : '').toLowerCase().trim();
            \\
            \\      const visibleRowIndices = [];
            \\      for (let i = 0; i < tableDataResultData.rows.length; i++) {
            \\        const row = tableDataResultData.rows[i];
            \\        if (filterText) {
            \\          let match = false;
            \\          for (let c = 0; c < row.length; c++) {
            \\            const v = row[c];
            \\            if (v !== null && v !== undefined && String(v).toLowerCase().includes(filterText)) {
            \\              match = true;
            \\              break;
            \\            }
            \\          }
            \\          if (!match) continue;
            \\        }
            \\        visibleRowIndices.push(i);
            \\      }
            \\
            \\      if (tableDataSortCol) {
            \\        const colIdx = tableDataResultData.columns.findIndex(c => c.name.toLowerCase() === tableDataSortCol.toLowerCase());
            \\        if (colIdx !== -1) {
            \\          visibleRowIndices.sort((a, b) => {
            \\            let va = tableDataResultData.rows[a][colIdx];
            \\            let vb = tableDataResultData.rows[b][colIdx];
            \\            if (va === null || va === undefined) return tableDataSortAsc ? -1 : 1;
            \\            if (vb === null || vb === undefined) return tableDataSortAsc ? 1 : -1;
            \\            if (typeof va === 'number' && typeof vb === 'number') {
            \\              return tableDataSortAsc ? va - vb : vb - va;
            \\            }
            \\            va = String(va).toLowerCase();
            \\            vb = String(vb).toLowerCase();
            \\            if (va < vb) return tableDataSortAsc ? -1 : 1;
            \\            if (va > vb) return tableDataSortAsc ? 1 : -1;
            \\            return 0;
            \\          });
            \\        }
            \\      }
            \\
            \\      let tbHtml = '';
            \\      for (let v = 0; v < visibleRowIndices.length; v++) {
            \\        const rowIdx = visibleRowIndices[v];
            \\        const row = tableDataResultData.rows[rowIdx];
            \\        const isSelected = tableDataSelectedRows.has(rowIdx);
            \\        const selClass = isSelected ? ' class="selected"' : '';
            \\
            \\        tbHtml += '<tr' + selClass + ' onclick="onTableDataRowClick(' + rowIdx + ', event)" ondblclick="openTableDataDetailModal(' + rowIdx + ')">';
            \\        tbHtml += '<td style="text-align: center;"><input type="checkbox" ' + (isSelected ? 'checked' : '') + ' onclick="event.stopPropagation(); toggleTableDataRowSelection(' + rowIdx + ', this.checked)"></td>';
            \\        tbHtml += '<td style="text-align: center; color: #64748b; font-family: monospace; font-size: 11px;">' + (v + 1) + '</td>';
            \\
            \\        for (let c = 0; c < tableDataResultData.columns.length; c++) {
            \\          const col = tableDataResultData.columns[c];
            \\          const val = row[c];
            \\          let cellHtml = '';
            \\          if (val === null || val === undefined) {
            \\            cellHtml = '<span class="table-data-null-val">NULL</span>';
            \\          } else if (typeof val === 'boolean') {
            \\            cellHtml = val ? '<span class="table-data-bool-true">TRUE</span>' : '<span class="table-data-bool-false">FALSE</span>';
            \\          } else {
            \\            cellHtml = escapeHtml(String(val));
            \\          }
            \\          const pkStyle = col.primary_key ? 'font-weight: 600; color: #f8fafc;' : '';
            \\          tbHtml += '<td style="' + pkStyle + '">' + cellHtml + '</td>';
            \\        }
            \\        tbHtml += '</tr>';
            \\      }
            \\      if (visibleRowIndices.length === 0) {
            \\        tbHtml = '<tr><td colspan="' + (tableDataResultData.columns.length + 2) + '" style="text-align:center; padding: 30px; color: #94a3b8;">No records match display criteria.</td></tr>';
            \\      }
            \\      document.getElementById('tableDataGridTbody').innerHTML = tbHtml;
            \\      updateTableDataToolbarState();
            \\    }
            \\
            \\    function updateTableDataToolbarState() {
            \\      const total = tableDataResultData ? tableDataResultData.total_hits : 0;
            \\      const returned = tableDataResultData ? tableDataResultData.rows.length : 0;
            \\      const selCount = tableDataSelectedRows.size;
            \\      const hitsEl = document.getElementById('tableDataStatusHitsText');
            \\      if (hitsEl) hitsEl.innerText = returned + ' of ' + total + ' records displayed';
            \\      const selEl = document.getElementById('tableDataStatusSelectedText');
            \\      if (selEl) selEl.innerText = selCount + ' selected';
            \\
            \\      const selectAll = document.getElementById('tableDataSelectAllCheckbox');
            \\      if (selectAll && tableDataResultData && tableDataResultData.rows.length > 0) {
            \\        selectAll.checked = (selCount === tableDataResultData.rows.length);
            \\      }
            \\
            \\      const btnNew = document.getElementById('tableDataNewRowBtn');
            \\      const btnEdit = document.getElementById('tableDataEditRowBtn');
            \\      const btnClone = document.getElementById('tableDataCloneRowBtn');
            \\      const btnDelete = document.getElementById('tableDataDeleteRowBtn');
            \\
            \\      if (btnNew) btnNew.disabled = !tableDataEditMode;
            \\      if (btnEdit) btnEdit.disabled = !tableDataEditMode || (selCount !== 1);
            \\      if (btnClone) btnClone.disabled = !tableDataEditMode || (selCount !== 1);
            \\      if (btnDelete) btnDelete.disabled = !tableDataEditMode || (selCount === 0);
            \\    }
            \\
            \\    function onTableDataRowClick(rowIdx, event) {
            \\      if (event.ctrlKey || event.metaKey) {
            \\        if (tableDataSelectedRows.has(rowIdx)) tableDataSelectedRows.delete(rowIdx);
            \\        else tableDataSelectedRows.add(rowIdx);
            \\      } else if (event.shiftKey && tableDataSelectedRows.size > 0) {
            \\        const last = Array.from(tableDataSelectedRows).pop();
            \\        const min = Math.min(last, rowIdx);
            \\        const max = Math.max(last, rowIdx);
            \\        for (let i = min; i <= max; i++) tableDataSelectedRows.add(i);
            \\      } else {
            \\        if (tableDataSelectedRows.size === 1 && tableDataSelectedRows.has(rowIdx)) {
            \\          tableDataSelectedRows.clear();
            \\        } else {
            \\          tableDataSelectedRows.clear();
            \\          tableDataSelectedRows.add(rowIdx);
            \\        }
            \\      }
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function toggleTableDataRowSelection(rowIdx, checked) {
            \\      if (checked) tableDataSelectedRows.add(rowIdx);
            \\      else tableDataSelectedRows.delete(rowIdx);
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function toggleSelectAllTableDataRows(checked) {
            \\      if (!tableDataResultData || !tableDataResultData.rows) return;
            \\      if (checked) {
            \\        for (let i = 0; i < tableDataResultData.rows.length; i++) {
            \\          tableDataSelectedRows.add(i);
            \\        }
            \\      } else {
            \\        tableDataSelectedRows.clear();
            \\      }
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function setTableDataEditMode(active) {
            \\      tableDataEditMode = !!active;
            \\      const dispBtn = document.getElementById('tableDataModeDisplayBtn');
            \\      const editBtn = document.getElementById('tableDataModeEditBtn');
            \\      const banner = document.getElementById('tableDataEditBanner');
            \\      if (active) {
            \\        if (dispBtn) dispBtn.classList.remove('active');
            \\        if (editBtn) editBtn.classList.add('active');
            \\        if (banner) banner.style.display = 'flex';
            \\      } else {
            \\        if (dispBtn) dispBtn.classList.add('active');
            \\        if (editBtn) editBtn.classList.remove('active');
            \\        if (banner) banner.style.display = 'none';
            \\      }
            \\      updateTableDataToolbarState();
            \\    }
            \\
            \\    function cycleTableDataHeaderFormat() {
            \\      if (tableDataHeaderFormat === 'both') tableDataHeaderFormat = 'tech';
            \\      else if (tableDataHeaderFormat === 'tech') tableDataHeaderFormat = 'text';
            \\      else tableDataHeaderFormat = 'both';
            \\
            \\      const select = document.getElementById('tableDataHeaderDisplaySelect');
            \\      if (select) select.value = tableDataHeaderFormat;
            \\      const label = document.getElementById('tableDataHeaderModeLabel');
            \\      if (label) label.innerText = tableDataHeaderFormat === 'both' ? 'Both' : (tableDataHeaderFormat === 'tech' ? 'Technical' : 'Description');
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function onTableDataHeaderFormatChange(val) {
            \\      tableDataHeaderFormat = val;
            \\      const label = document.getElementById('tableDataHeaderModeLabel');
            \\      if (label) label.innerText = tableDataHeaderFormat === 'both' ? 'Both' : (tableDataHeaderFormat === 'tech' ? 'Technical' : 'Description');
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function filterTableDataGridLocal() {
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function sortTableDataGridByCol(colName) {
            \\      if (tableDataSortCol === colName) {
            \\        tableDataSortAsc = !tableDataSortAsc;
            \\      } else {
            \\        tableDataSortCol = colName;
            \\        tableDataSortAsc = true;
            \\      }
            \\      renderTableDataGrid();
            \\    }
            \\
            \\    function openTableDataDetailModal(rowIdx) {
            \\      if (!tableDataResultData || !tableDataResultData.rows || tableDataResultData.rows.length === 0) return;
            \\      if (rowIdx === undefined) {
            \\        if (tableDataSelectedRows.size > 0) {
            \\          rowIdx = Array.from(tableDataSelectedRows)[0];
            \\        } else {
            \\          rowIdx = 0;
            \\        }
            \\      }
            \\      tableDataDetailIndex = rowIdx;
            \\      renderTableDataDetailView();
            \\      document.getElementById('tableDataDetailModal').style.display = 'flex';
            \\    }
            \\
            \\    function renderTableDataDetailView() {
            \\      if (!tableDataResultData || !tableDataResultData.rows || tableDataDetailIndex < 0 || tableDataDetailIndex >= tableDataResultData.rows.length) return;
            \\      const row = tableDataResultData.rows[tableDataDetailIndex];
            \\      const origIdx = tableDataResultData.row_indices[tableDataDetailIndex];
            \\      document.getElementById('tableDataDetailIndexBadge').innerText = (tableDataDetailIndex + 1) + ' / ' + tableDataResultData.rows.length + ' (Index: #' + origIdx + ')';
            \\
            \\      let html = '<table class="table-data-detail-table">';
            \\      html += '<thead><tr><th>Field</th><th>Type</th><th>Description</th><th>Value</th></tr></thead><tbody>';
            \\      for (let c = 0; c < tableDataResultData.columns.length; c++) {
            \\        const col = tableDataResultData.columns[c];
            \\        const val = row[c];
            \\        const isPk = !!col.primary_key;
            \\        html += '<tr>';
            \\        html += '<td style="font-family:monospace;font-weight:600;color:#f8fafc;">' + (isPk ? '🔑 ' : '') + escapeHtml(col.name) + '</td>';
            \\        html += '<td><span class="badge-type">' + escapeHtml(col.type) + '</span></td>';
            \\        html += '<td style="color:#94a3b8;font-size:12px;">' + escapeHtml(col.description || '-') + '</td>';
            \\        let valDisp = '';
            \\        if (val === null || val === undefined) {
            \\          valDisp = '<span class="table-data-null-val">NULL</span>';
            \\        } else if (typeof val === 'boolean') {
            \\          valDisp = val ? '<span class="table-data-bool-true">TRUE</span>' : '<span class="table-data-bool-false">FALSE</span>';
            \\        } else {
            \\          valDisp = escapeHtml(String(val));
            \\        }
            \\        html += '<td style="font-family:monospace;word-break:break-all;">' + valDisp + '</td>';
            \\        html += '</tr>';
            \\      }
            \\      html += '</tbody></table>';
            \\      document.getElementById('tableDataDetailModalBody').innerHTML = html;
            \\    }
            \\
            \\    function prevTableDataDetailRecord() {
            \\      if (!tableDataResultData) return;
            \\      if (tableDataDetailIndex > 0) {
            \\        tableDataDetailIndex--;
            \\        renderTableDataDetailView();
            \\      }
            \\    }
            \\
            \\    function nextTableDataDetailRecord() {
            \\      if (!tableDataResultData) return;
            \\      if (tableDataDetailIndex < tableDataResultData.rows.length - 1) {
            \\        tableDataDetailIndex++;
            \\        renderTableDataDetailView();
            \\      }
            \\    }
            \\
            \\    function openTableDataNewModal() {
            \\      if (!tableDataTable) return;
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\      let html = '<div style="display: flex; flex-direction: column; gap: 14px;">';
            \\      for (const col of tbl.columns) {
            \\        const isPk = !!col.primary_key;
            \\        const isAuto = !!col.auto_increment;
            \\        const req = (!col.nullable && !isAuto) ? '<span style="color:#ef4444;">*</span>' : '';
            \\        html += '<div class="form-group" style="margin-bottom:0;">';
            \\        html += '<label class="form-label" style="display:flex;justify-content:space-between;">';
            \\        html += '<span>' + (isPk ? '🔑 ' : '') + '<strong>' + escapeHtml(col.name) + '</strong> ' + req + ' <span class="badge-type">' + escapeHtml(col.type) + '</span></span>';
            \\        if (col.description) html += '<span style="font-size:11px;color:#94a3b8;font-weight:normal;">' + escapeHtml(col.description) + '</span>';
            \\        html += '</label>';
            \\        if (isAuto) {
            \\          html += '<input type="text" id="table_data_new_' + escapeHtml(col.name) + '" class="form-control" placeholder="(Auto-increment assigned by StrataDB)" style="font-family:monospace;">';
            \\        } else {
            \\          html += '<input type="text" id="table_data_new_' + escapeHtml(col.name) + '" class="form-control" placeholder="' + escapeHtml(col.type) + ' value" style="font-family:monospace;">';
            \\        }
            \\        html += '</div>';
            \\      }
            \\      html += '</div>';
            \\      document.getElementById('tableDataNewModalBody').innerHTML = html;
            \\      document.getElementById('tableDataNewModal').style.display = 'flex';
            \\    }
            \\
            \\    async function submitTableDataNewEntry() {
            \\      if (!tableDataTable) return;
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\      const fields = [];
            \\      for (const col of tbl.columns) {
            \\        const inp = document.getElementById('table_data_new_' + col.name);
            \\        if (inp) {
            \\          const val = inp.value.trim();
            \\          if (val !== '') {
            \\            fields.push({ column: col.name, value: val });
            \\          }
            \\        }
            \\      }
            \\      try {
            \\        const res = await fetch('/api/table_data/insert', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: tableDataTable,
            \\            fields: fields
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          alert('Insert failed: ' + data.error);
            \\          return;
            \\        }
            \\        closeTableDataModal('tableDataNewModal');
            \\        executeTableDataQuery();
            \\        await fetchTables();
            \\      } catch (e) {
            \\        alert('Network error inserting entry: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function openTableDataEditModal() {
            \\      if (tableDataSelectedRows.size !== 1) {
            \\        showGridAlert('error', 'Please select exactly one record to edit.');
            \\        return;
            \\      }
            \\      const rowIdx = Array.from(tableDataSelectedRows)[0];
            \\      const row = tableDataResultData.rows[rowIdx];
            \\      const origIdx = tableDataResultData.row_indices[rowIdx];
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\
            \\      let html = '<div style="display: flex; flex-direction: column; gap: 14px;">';
            \\      html += '<div style="font-size:12px;color:#94a3b8;padding-bottom:6px;border-bottom:1px solid #334155;">Editing row at index <strong>#' + origIdx + '</strong>:</div>';
            \\
            \\      for (const col of tbl.columns) {
            \\        const isPk = !!col.primary_key;
            \\        const colIdx = tableDataResultData.columns.findIndex(c => c.name.toLowerCase() === col.name.toLowerCase());
            \\        const val = (colIdx !== -1) ? row[colIdx] : '';
            \\        const valStr = (val === null || val === undefined) ? '' : String(val);
            \\
            \\        html += '<div class="form-group" style="margin-bottom:0;">';
            \\        html += '<label class="form-label" style="display:flex;justify-content:space-between;">';
            \\        html += '<span>' + (isPk ? '🔑 ' : '') + '<strong>' + escapeHtml(col.name) + '</strong> <span class="badge-type">' + escapeHtml(col.type) + '</span></span>';
            \\        if (col.description) html += '<span style="font-size:11px;color:#94a3b8;font-weight:normal;">' + escapeHtml(col.description) + '</span>';
            \\        html += '</label>';
            \\        if (isPk) {
            \\          html += '<input type="text" id="table_data_edit_' + escapeHtml(col.name) + '" class="form-control" value="' + escapeHtml(valStr) + '" disabled style="font-family:monospace;background:#0f172a;opacity:0.75;" title="Primary Key cannot be modified in edit mode">';
            \\        } else {
            \\          html += '<input type="text" id="table_data_edit_' + escapeHtml(col.name) + '" class="form-control" value="' + escapeHtml(valStr) + '" placeholder="NULL (leave blank for NULL)" style="font-family:monospace;">';
            \\        }
            \\        html += '</div>';
            \\      }
            \\      html += '</div>';
            \\      document.getElementById('tableDataEditModalBody').innerHTML = html;
            \\      document.getElementById('tableDataEditModal').dataset.rowIndex = origIdx;
            \\      document.getElementById('tableDataEditModal').style.display = 'flex';
            \\    }
            \\
            \\    async function submitTableDataEditEntry() {
            \\      if (!tableDataTable) return;
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\      const modal = document.getElementById('tableDataEditModal');
            \\      const origIdx = parseInt(modal.dataset.rowIndex);
            \\      const fields = [];
            \\      for (const col of tbl.columns) {
            \\        const inp = document.getElementById('table_data_edit_' + col.name);
            \\        if (inp) {
            \\          fields.push({ column: col.name, value: inp.value });
            \\        }
            \\      }
            \\      try {
            \\        const res = await fetch('/api/table_data/update', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify({
            \\            table: tableDataTable,
            \\            row_index: origIdx,
            \\            fields: fields
            \\          })
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          alert('Update failed: ' + data.error);
            \\          return;
            \\        }
            \\        closeTableDataModal('tableDataEditModal');
            \\        executeTableDataQuery();
            \\      } catch (e) {
            \\        alert('Network error updating entry: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function cloneTableDataSelectedRow() {
            \\      if (tableDataSelectedRows.size !== 1) {
            \\        showGridAlert('error', 'Please select exactly one record to duplicate.');
            \\        return;
            \\      }
            \\      const rowIdx = Array.from(tableDataSelectedRows)[0];
            \\      const row = tableDataResultData.rows[rowIdx];
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      if (!tbl) return;
            \\
            \\      openTableDataNewModal();
            \\      for (const col of tbl.columns) {
            \\        const isPk = !!col.primary_key;
            \\        const isAuto = !!col.auto_increment;
            \\        const colIdx = tableDataResultData.columns.findIndex(c => c.name.toLowerCase() === col.name.toLowerCase());
            \\        const val = (colIdx !== -1) ? row[colIdx] : '';
            \\        const valStr = (val === null || val === undefined) ? '' : String(val);
            \\        const inp = document.getElementById('table_data_new_' + col.name);
            \\        if (inp) {
            \\          if (isPk && isAuto) {
            \\            inp.value = '';
            \\          } else {
            \\            inp.value = valStr;
            \\          }
            \\        }
            \\      }
            \\    }
            \\
            \\    async function deleteTableDataSelectedRows() {
            \\      if (tableDataSelectedRows.size === 0) {
            \\        showGridAlert('error', 'No records selected for deletion.');
            \\        return;
            \\      }
            \\      const count = tableDataSelectedRows.size;
            \\      if (!confirm('Are you sure you want to delete ' + count + ' record(s) from table \'' + tableDataTable + '\'?\n\nThis deletion will be logged to CDC/WAL and synchronized across all cluster nodes.')) {
            \\        return;
            \\      }
            \\      const indices = [];
            \\      const pks = [];
            \\      const tbl = knownTables ? knownTables.find(t => t.name.toLowerCase() === tableDataTable.toLowerCase()) : null;
            \\      const pkCol = tbl ? tbl.columns.find(c => c.primary_key) : null;
            \\      const pkColIdx = pkCol ? tableDataResultData.columns.findIndex(c => c.name.toLowerCase() === pkCol.name.toLowerCase()) : -1;
            \\
            \\      for (const rowIdx of tableDataSelectedRows) {
            \\        const origIdx = tableDataResultData.row_indices[rowIdx];
            \\        indices.push(origIdx);
            \\        if (pkColIdx !== -1) {
            \\          const pkVal = tableDataResultData.rows[rowIdx][pkColIdx];
            \\          if (pkVal !== null && pkVal !== undefined) pks.push(String(pkVal));
            \\        }
            \\      }
            \\
            \\      try {
            \\        const payload = {
            \\          table: tableDataTable,
            \\          row_indices: indices
            \\        };
            \\        if (pks.length === indices.length) {
            \\          payload.pk_values = pks;
            \\        }
            \\        const res = await fetch('/api/table_data/delete', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify(payload)
            \\        });
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          alert('Delete failed: ' + data.error);
            \\          return;
            \\        }
            \\        tableDataSelectedRows.clear();
            \\        executeTableDataQuery();
            \\        await fetchTables();
            \\      } catch (e) {
            \\        alert('Network error deleting entries: ' + e.message);
            \\      }
            \\    }
            \\
            \\    function exportTableDataCsv() {
            \\      if (!tableDataResultData || !tableDataResultData.rows) return;
            \\      const headers = tableDataResultData.columns.map(c => '"' + c.name.replace(/"/g, '""') + '"');
            \\      let csv = headers.join(',') + '\n';
            \\      for (const row of tableDataResultData.rows) {
            \\        const line = row.map(val => {
            \\          if (val === null || val === undefined) return '';
            \\          const str = String(val);
            \\          if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            \\            return '"' + str.replace(/"/g, '""') + '"';
            \\          }
            \\          return str;
            \\        });
            \\        csv += line.join(',') + '\n';
            \\      }
            \\      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
            \\      const url = URL.createObjectURL(blob);
            \\      const a = document.createElement('a');
            \\      a.href = url;
            \\      a.download = tableDataTable + '_data_export.csv';
            \\      a.click();
            \\      URL.revokeObjectURL(url);
            \\    }
            \\
            \\    function exportTableDataJson() {
            \\      if (!tableDataResultData || !tableDataResultData.rows) return;
            \\      const out = [];
            \\      for (const row of tableDataResultData.rows) {
            \\        const obj = {};
            \\        for (let c = 0; c < tableDataResultData.columns.length; c++) {
            \\          obj[tableDataResultData.columns[c].name] = row[c];
            \\        }
            \\        out.push(obj);
            \\      }
            \\      const blob = new Blob([JSON.stringify(out, null, 2)], { type: 'application/json' });
            \\      const url = URL.createObjectURL(blob);
            \\      const a = document.createElement('a');
            \\      a.href = url;
            \\      a.download = tableDataTable + '_data_export.json';
            \\      a.click();
            \\      URL.revokeObjectURL(url);
            \\    }
            \\
            \\    function copyTableDataClipboard() {
            \\      if (!tableDataResultData || !tableDataResultData.rows) return;
            \\      const headers = tableDataResultData.columns.map(c => c.name).join('\t');
            \\      let tsv = headers + '\n';
            \\      for (const row of tableDataResultData.rows) {
            \\        tsv += row.map(val => (val === null || val === undefined) ? '' : String(val).replace(/\t/g, ' ')).join('\t') + '\n';
            \\      }
            \\      navigator.clipboard.writeText(tsv).then(() => {
            \\        showGridAlert('success', 'Copied ' + tableDataResultData.rows.length + ' rows to clipboard in Excel TSV format!');
            \\      }).catch(e => {
            \\        alert('Failed to copy to clipboard: ' + e.message);
            \\      });
            \\    }
            \\
            \\    function jumpToStudioFromTableData() {
            \\      if (tableDataTable) {
            \\        openAmendMode(tableDataTable);
            \\      } else {
            \\        switchMainTab('studio');
            \\      }
            \\    }
            \\
            \\    function openTableInTableData(tableName) {
            \\      if (!tableName) return;
            \\      switchMainTab('tableData');
            \\      updateTableDataTableSelect(tableName);
            \\      executeTableDataQuery();
            \\    }
            \\
            \\    function closeTableDataModal(id) {
            \\      const el = document.getElementById(id);
            \\      if (el) el.style.display = 'none';
            \\    }
            \\
            \\    // --- Excel / CSV Import Functionality ---
            \\    let excelRawData = null; // { headers: [...], rows: [[...], ...] }
            \\    let excelWorkbook = null;
            \\    let excelImportColumns = []; // [{ name, type, is_primary_key, auto_increment, nullable, description, sourceIdx }]
            \\    let excelImportSourceTab = 'file';
            \\
            \\    function openExcelImportModal() {
            \\      excelRawData = null;
            \\      excelWorkbook = null;
            \\      excelImportColumns = [];
            \\      excelImportSourceTab = 'file';
            \\
            \\      document.getElementById('excelFileInput').value = '';
            \\      document.getElementById('excelFileSelectedInfo').style.display = 'none';
            \\      document.getElementById('excelFileSelectedInfo').innerText = '';
            \\      document.getElementById('excelSheetSelectorRow').style.display = 'none';
            \\      document.getElementById('excelSheetSelect').innerHTML = '';
            \\      document.getElementById('excelPasteInput').value = '';
            \\      document.getElementById('excelImportTableName').value = '';
            \\      document.getElementById('excelImportTableDesc').value = '';
            \\      document.getElementById('excelImportAddIdPk').checked = true;
            \\      document.getElementById('excelImportColumnsContainer').style.display = 'none';
            \\      document.getElementById('excelImportColumnsTableBody').innerHTML = '';
            \\      document.getElementById('excelImportPreviewContainer').style.display = 'none';
            \\      document.getElementById('excelImportPreviewThead').innerHTML = '';
            \\      document.getElementById('excelImportPreviewTbody').innerHTML = '';
            \\      document.getElementById('excelImportSubmitBtn').disabled = true;
            \\      document.getElementById('excelImportStatusText').innerText = '';
            \\      hideExcelImportAlert();
            \\
            \\      switchImportSourceTab('file');
            \\      document.getElementById('excelImportModal').style.display = 'flex';
            \\      setupDropzone();
            \\    }
            \\
            \\    function closeExcelImportModal() {
            \\      document.getElementById('excelImportModal').style.display = 'none';
            \\    }
            \\
            \\    function showExcelImportAlert(type, message) {
            \\      const el = document.getElementById('excelImportAlert');
            \\      el.className = 'alert-box ' + type;
            \\      el.innerText = message;
            \\      el.style.display = 'block';
            \\    }
            \\
            \\    function hideExcelImportAlert() {
            \\      const el = document.getElementById('excelImportAlert');
            \\      el.style.display = 'none';
            \\      el.innerText = '';
            \\    }
            \\
            \\    function switchImportSourceTab(tab) {
            \\      excelImportSourceTab = tab;
            \\      const fileBtn = document.getElementById('importTabFileBtn');
            \\      const pasteBtn = document.getElementById('importTabPasteBtn');
            \\      const fileSec = document.getElementById('importFileSection');
            \\      const pasteSec = document.getElementById('importPasteSection');
            \\
            \\      if (tab === 'file') {
            \\        fileBtn.classList.add('active');
            \\        pasteBtn.classList.remove('active');
            \\        fileSec.style.display = 'block';
            \\        pasteSec.style.display = 'none';
            \\      } else {
            \\        pasteBtn.classList.add('active');
            \\        fileBtn.classList.remove('active');
            \\        pasteSec.style.display = 'block';
            \\        fileSec.style.display = 'none';
            \\      }
            \\    }
            \\
            \\    let dropzoneInitialized = false;
            \\    function setupDropzone() {
            \\      if (dropzoneInitialized) return;
            \\      dropzoneInitialized = true;
            \\      const dropzone = document.getElementById('excelDropzone');
            \\      ['dragenter', 'dragover'].forEach(name => {
            \\        dropzone.addEventListener(name, (e) => {
            \\          e.preventDefault();
            \\          e.stopPropagation();
            \\          dropzone.classList.add('dragover');
            \\        });
            \\      });
            \\      ['dragleave', 'drop'].forEach(name => {
            \\        dropzone.addEventListener(name, (e) => {
            \\          e.preventDefault();
            \\          e.stopPropagation();
            \\          dropzone.classList.remove('dragover');
            \\        });
            \\      });
            \\      dropzone.addEventListener('drop', (e) => {
            \\        if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0) {
            \\          handleExcelFile(e.dataTransfer.files[0]);
            \\        }
            \\      });
            \\    }
            \\
            \\    function handleExcelFileInput(e) {
            \\      if (e.target.files && e.target.files.length > 0) {
            \\        handleExcelFile(e.target.files[0]);
            \\      }
            \\    }
            \\
            \\    function sanitizeIdentifier(name) {
            \\      if (!name) return 'col';
            \\      let s = String(name).trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '');
            \\      if (!s) return 'col';
            \\      if (/^[0-9]/.test(s)) s = 'col_' + s;
            \\      return s;
            \\    }
            \\
            \\    function sanitizeImportTableName(input) {
            \\      input.value = input.value.replace(/[^a-zA-Z0-9_]/g, '_');
            \\    }
            \\
            \\    function handleExcelFile(file) {
            \\      if (!file) return;
            \\      hideExcelImportAlert();
            \\      const infoEl = document.getElementById('excelFileSelectedInfo');
            \\      infoEl.innerText = '📄 ' + file.name + ' (' + (file.size / 1024).toFixed(1) + ' KB)';
            \\      infoEl.style.display = 'block';
            \\
            \\      const baseName = file.name.replace(/\\.[^/.]+$/, '');
            \\      const suggestedName = sanitizeIdentifier(baseName);
            \\      if (!document.getElementById('excelImportTableName').value) {
            \\        document.getElementById('excelImportTableName').value = suggestedName;
            \\      }
            \\
            \\      document.getElementById('excelImportStatusText').innerText = 'Reading file...';
            \\
            \\      const fileNameLower = (file.name || '').toLowerCase();
            \\      const fileTypeLower = (file.type || '').toLowerCase();
            \\      const isDelimited = fileNameLower.endsWith('.csv') || fileNameLower.endsWith('.tsv') || fileNameLower.endsWith('.txt') || fileTypeLower.includes('csv') || fileTypeLower.includes('tab-separated');
            \\
            \\      if (window.XLSX && !isDelimited) {
            \\        const reader = new FileReader();
            \\        reader.onload = function(e) {
            \\          try {
            \\            const data = new Uint8Array(e.target.result);
            \\            excelWorkbook = XLSX.read(data, { type: 'array' });
            \\            const sheetNames = excelWorkbook.SheetNames;
            \\            if (!sheetNames || sheetNames.length === 0) {
            \\              showExcelImportAlert('error', 'Workbook contains no sheets.');
            \\              return;
            \\            }
            \\            const selectEl = document.getElementById('excelSheetSelect');
            \\            selectEl.innerHTML = '';
            \\            sheetNames.forEach(sheet => {
            \\              const opt = document.createElement('option');
            \\              opt.value = sheet;
            \\              opt.textContent = sheet;
            \\              selectEl.appendChild(opt);
            \\            });
            \\            document.getElementById('excelSheetSelectorRow').style.display = sheetNames.length > 1 ? 'block' : 'none';
            \\            loadSheetData(sheetNames[0]);
            \\          } catch (err) {
            \\            console.error(err);
            \\            showExcelImportAlert('error', 'Failed to parse Excel file: ' + err.message);
            \\          }
            \\        };
            \\        reader.onerror = () => showExcelImportAlert('error', 'Failed to read file.');
            \\        reader.readAsArrayBuffer(file);
            \\      } else {
            \\        const reader = new FileReader();
            \\        reader.onload = function(e) {
            \\          try {
            \\            const text = e.target.result;
            \\            document.getElementById('excelSheetSelectorRow').style.display = 'none';
            \\            parseAndProcessDelimitedText(text);
            \\          } catch (err) {
            \\            showExcelImportAlert('error', 'Failed to parse text: ' + err.message);
            \\          }
            \\        };
            \\        reader.onerror = () => showExcelImportAlert('error', 'Failed to read file.');
            \\        reader.readAsText(file);
            \\      }
            \\    }
            \\
            \\    function onExcelSheetSelectChange(sheetName) {
            \\      if (!excelWorkbook) return;
            \\      loadSheetData(sheetName);
            \\    }
            \\
            \\    function loadSheetData(sheetName) {
            \\      if (!excelWorkbook || !excelWorkbook.Sheets[sheetName]) return;
            \\      const worksheet = excelWorkbook.Sheets[sheetName];
            \\      const rawRows = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: null, raw: false });
            \\      if (!rawRows || rawRows.length === 0) {
            \\        showExcelImportAlert('error', 'Selected sheet has no data rows.');
            \\        return;
            \\      }
            \\      processRawGrid(rawRows);
            \\    }
            \\
            \\    function onExcelPasteInput() {
            \\      const text = document.getElementById('excelPasteInput').value;
            \\      if (!text || !text.trim()) {
            \\        excelRawData = null;
            \\        document.getElementById('excelImportColumnsContainer').style.display = 'none';
            \\        document.getElementById('excelImportPreviewContainer').style.display = 'none';
            \\        document.getElementById('excelImportSubmitBtn').disabled = true;
            \\        return;
            \\      }
            \\      parseAndProcessDelimitedText(text);
            \\    }
            \\
            \\    function parseAndProcessDelimitedText(text) {
            \\      const grid = parseDelimitedText(text);
            \\      if (!grid || grid.length === 0) {
            \\        showExcelImportAlert('error', 'No valid rows found in pasted data.');
            \\        return;
            \\      }
            \\      processRawGrid(grid);
            \\    }
            \\
            \\    function parseDelimitedText(text) {
            \\      if (!text) return [];
            \\      // Strip UTF-8 BOM if present at start
            \\      if (text.charCodeAt(0) === 0xFEFF) {
            \\        text = text.slice(1);
            \\      }
            \\
            \\      // Auto-detect delimiter from the first line: tab, comma, or semicolon
            \\      let firstLineEnd = text.indexOf('\n');
            \\      if (firstLineEnd === -1) firstLineEnd = text.length;
            \\      let firstLine = text.slice(0, firstLineEnd);
            \\      if (firstLine.endsWith('\r')) firstLine = firstLine.slice(0, -1);
            \\
            \\      const tabCount = (firstLine.match(/\t/g) || []).length;
            \\      const commaCount = (firstLine.match(/,/g) || []).length;
            \\      const semiCount = (firstLine.match(/;/g) || []).length;
            \\      let delimiter = ',';
            \\      if (tabCount > commaCount && tabCount > semiCount) {
            \\        delimiter = '\t';
            \\      } else if (semiCount > commaCount && semiCount > tabCount) {
            \\        delimiter = ';';
            \\      }
            \\
            \\      // Full RFC 4180 streaming parser
            \\      const rows = [];
            \\      let currentRow = [];
            \\      let currentCell = '';
            \\      let inQuotes = false;
            \\      let i = 0;
            \\      const len = text.length;
            \\
            \\      while (i < len) {
            \\        const char = text[i];
            \\
            \\        if (char === '"') {
            \\          if (inQuotes && i + 1 < len && text[i + 1] === '"') {
            \\            currentCell += '"';
            \\            i += 2;
            \\            continue;
            \\          } else {
            \\            inQuotes = !inQuotes;
            \\            i++;
            \\            continue;
            \\          }
            \\        }
            \\
            \\        if (!inQuotes) {
            \\          if (char === delimiter) {
            \\            currentRow.push(currentCell.trim());
            \\            currentCell = '';
            \\            i++;
            \\            continue;
            \\          } else if (char === '\r') {
            \\            if (i + 1 < len && text[i + 1] === '\n') {
            \\              i++;
            \\            }
            \\            currentRow.push(currentCell.trim());
            \\            currentCell = '';
            \\            if (currentRow.length > 1 || (currentRow.length === 1 && currentRow[0] !== '')) {
            \\              rows.push(currentRow);
            \\            }
            \\            currentRow = [];
            \\            i++;
            \\            continue;
            \\          } else if (char === '\n') {
            \\            currentRow.push(currentCell.trim());
            \\            currentCell = '';
            \\            if (currentRow.length > 1 || (currentRow.length === 1 && currentRow[0] !== '')) {
            \\              rows.push(currentRow);
            \\            }
            \\            currentRow = [];
            \\            i++;
            \\            continue;
            \\          }
            \\        }
            \\
            \\        currentCell += char;
            \\        i++;
            \\      }
            \\
            \\      if (currentCell.length > 0 || inQuotes || currentRow.length > 0) {
            \\        currentRow.push(currentCell.trim());
            \\        if (currentRow.length > 1 || (currentRow.length === 1 && currentRow[0] !== '')) {
            \\          rows.push(currentRow);
            \\        }
            \\      }
            \\
            \\      return rows;
            \\    }
            \\
            \\    function processRawGrid(rawGrid) {
            \\      if (!rawGrid || rawGrid.length === 0) {
            \\        showExcelImportAlert('error', 'CSV/Delimited data is empty.');
            \\        return;
            \\      }
            \\      hideExcelImportAlert();
            \\
            \\      // STRICT CONTRACT:
            \\      // Line 1 is used exclusively for field names.
            \\      // Data starts on Line 2 (Line 2 is Row 1 of data).
            \\      const rawHeaders = rawGrid[0] || [];
            \\      const headers = rawHeaders.map((h, idx) => {
            \\        const val = (h !== null && h !== undefined) ? String(h).trim() : '';
            \\        return val.length > 0 ? val : ('field_' + (idx + 1));
            \\      });
            \\
            \\      // Data records strictly start on Line 2 (index 1 of rawGrid)
            \\      const rawDataRows = rawGrid.slice(1);
            \\      const rows = rawDataRows.filter(r => r && r.some(c => c !== null && c !== undefined && String(c).trim() !== ''));
            \\
            \\      excelRawData = { headers, rows };
            \\      buildImportSchema();
            \\    }
            \\
            \\    function inferColumnType(values) {
            \\      let nonNull = 0;
            \\      let intCount = 0;
            \\      let realCount = 0;
            \\      let boolCount = 0;
            \\      let tsCount = 0;
            \\
            \\      for (let i = 0; i < Math.min(values.length, 100); i++) {
            \\        const v = values[i];
            \\        if (v === null || v === undefined) continue;
            \\        const s = String(v).trim();
            \\        if (s === '') continue;
            \\        nonNull++;
            \\
            \\        if (/^-?\\d+$/.test(s)) {
            \\          intCount++;
            \\        } else if (/^-?\\d+\\.\\d+$/.test(s)) {
            \\          realCount++;
            \\        } else if (/^(true|false|yes|no)$/i.test(s)) {
            \\          boolCount++;
            \\        } else if (/^\\d{4}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(?::\\d{2})?)?/.test(s)) {
            \\          tsCount++;
            \\        }
            \\      }
            \\
            \\      if (nonNull === 0) return 'TEXT';
            \\      if (intCount === nonNull) return 'INTEGER';
            \\      if ((intCount + realCount) === nonNull) return 'REAL';
            \\      if (boolCount === nonNull) return 'BOOLEAN';
            \\      if (tsCount === nonNull) return 'TIMESTAMP';
            \\      return 'TEXT';
            \\    }
            \\
            \\    function buildImportSchema() {
            \\      if (!excelRawData) return;
            \\      const addIdPkCheckbox = document.getElementById('excelImportAddIdPk');
            \\      const addIdPk = addIdPkCheckbox ? addIdPkCheckbox.checked : true;
            \\      const usedNames = new Set();
            \\      const cols = [];
            \\
            \\      // Check if Line 1 already contains an 'id' column
            \\      const existingIdColIdx = excelRawData.headers.findIndex(h => {
            \\        const s = sanitizeIdentifier(h);
            \\        return s === 'id';
            \\      });
            \\
            \\      if (existingIdColIdx >= 0) {
            \\        // Line 1 already contains an 'id' column: designate it as PK directly
            \\        excelRawData.headers.forEach((hdr, idx) => {
            \\          let baseName = sanitizeIdentifier(hdr);
            \\          let colName = baseName;
            \\          let counter = 1;
            \\          while (usedNames.has(colName)) {
            \\            colName = baseName + '_' + counter++;
            \\          }
            \\          usedNames.add(colName);
            \\
            \\          const colValues = excelRawData.rows.map(r => r[idx]);
            \\          const inferredType = inferColumnType(colValues);
            \\          const isPk = (idx === existingIdColIdx);
            \\
            \\          cols.push({
            \\            name: colName,
            \\            type: inferredType,
            \\            is_primary_key: isPk,
            \\            auto_increment: isPk && (inferredType === 'INTEGER'),
            \\            nullable: !isPk,
            \\            description: '',
            \\            isGeneratedPk: false,
            \\            sourceIdx: idx
            \\          });
            \\        });
            \\      } else if (addIdPk) {
            \\        // Line 1 does not contain 'id': prepend auto-increment 'id' primary key
            \\        cols.push({
            \\          name: 'id',
            \\          type: 'INTEGER',
            \\          is_primary_key: true,
            \\          auto_increment: true,
            \\          nullable: false,
            \\          description: '',
            \\          isGeneratedPk: true,
            \\          sourceIdx: -1
            \\        });
            \\        usedNames.add('id');
            \\
            \\        excelRawData.headers.forEach((hdr, idx) => {
            \\          let baseName = sanitizeIdentifier(hdr);
            \\          let colName = baseName;
            \\          let counter = 1;
            \\          while (usedNames.has(colName)) {
            \\            colName = baseName + '_' + counter++;
            \\          }
            \\          usedNames.add(colName);
            \\
            \\          const colValues = excelRawData.rows.map(r => r[idx]);
            \\          const inferredType = inferColumnType(colValues);
            \\
            \\          cols.push({
            \\            name: colName,
            \\            type: inferredType,
            \\            is_primary_key: false,
            \\            auto_increment: false,
            \\            nullable: true,
            \\            description: '',
            \\            isGeneratedPk: false,
            \\            sourceIdx: idx
            \\          });
            \\        });
            \\      } else {
            \\        // No generated PK and no existing id: default first column as PK
            \\        excelRawData.headers.forEach((hdr, idx) => {
            \\          let baseName = sanitizeIdentifier(hdr);
            \\          let colName = baseName;
            \\          let counter = 1;
            \\          while (usedNames.has(colName)) {
            \\            colName = baseName + '_' + counter++;
            \\          }
            \\          usedNames.add(colName);
            \\
            \\          const colValues = excelRawData.rows.map(r => r[idx]);
            \\          const inferredType = inferColumnType(colValues);
            \\          const isFirst = (idx === 0);
            \\
            \\          cols.push({
            \\            name: colName,
            \\            type: inferredType,
            \\            is_primary_key: isFirst,
            \\            auto_increment: isFirst && (inferredType === 'INTEGER'),
            \\            nullable: !isFirst,
            \\            description: '',
            \\            isGeneratedPk: false,
            \\            sourceIdx: idx
            \\          });
            \\        });
            \\      }
            \\
            \\      excelImportColumns = cols;
            \\      renderExcelImportColumnsTable();
            \\      renderExcelImportPreviewTable();
            \\
            \\      document.getElementById('excelImportColumnsContainer').style.display = 'block';
            \\      document.getElementById('excelImportPreviewContainer').style.display = 'block';
            \\      document.getElementById('excelImportSubmitBtn').disabled = (excelRawData.rows.length === 0);
            \\      document.getElementById('excelImportStatusText').innerText = 'Ready to import ' + excelRawData.rows.length + ' data rows (starting on Line 2) across ' + excelImportColumns.length + ' columns.';
            \\    }
            \\
            \\    function toggleExcelImportIdPk(checked) {
            \\      buildImportSchema();
            \\    }
            \\
            \\    function onImportColumnPkToggle(idx, checked) {
            \\      if (idx >= 0 && idx < excelImportColumns.length) {
            \\        excelImportColumns[idx].is_primary_key = checked;
            \\        if (checked) {
            \\          excelImportColumns[idx].nullable = false;
            \\        } else {
            \\          excelImportColumns[idx].auto_increment = false;
            \\        }
            \\      }
            \\      renderExcelImportColumnsTable();
            \\      renderExcelImportPreviewTable();
            \\    }
            \\
            \\    function renderExcelImportColumnsTable() {
            \\      const tbody = document.getElementById('excelImportColumnsTableBody');
            \\      tbody.innerHTML = '';
            \\      const summaryEl = document.getElementById('excelImportColSummary');
            \\      summaryEl.innerText = excelImportColumns.length + ' columns configured';
            \\
            \\      excelImportColumns.forEach((col, idx) => {
            \\        const tr = document.createElement('tr');
            \\        if (col.isGeneratedPk) {
            \\          tr.style.background = 'rgba(56, 189, 248, 0.08)';
            \\        }
            \\
            \\        tr.innerHTML = `
            \\          <td>
            \\            <div style="display: flex; align-items: center; gap: 6px;">
            \\              <input type="text" class="form-control" style="padding: 4px 8px; font-size: 12px; font-family: monospace; font-weight: 600;" value="${col.name}" onchange="excelImportColumns[${idx}].name = sanitizeIdentifier(this.value); renderExcelImportPreviewTable();" ${col.isGeneratedPk ? 'readonly title="System-generated primary key"' : ''}>
            \\              ${col.isGeneratedPk ? '<span class="badge-type" style="background: rgba(56, 189, 248, 0.2); color: #38bdf8; font-size: 10px;">ID PK</span>' : ''}
            \\            </div>
            \\          </td>
            \\          <td>
            \\            <select class="form-control" style="padding: 4px 6px; font-size: 12px;" onchange="excelImportColumns[${idx}].type = this.value; if (this.value !== 'INTEGER') excelImportColumns[${idx}].auto_increment = false; renderExcelImportColumnsTable();" ${col.isGeneratedPk ? 'disabled' : ''}>
            \\              <option value="TEXT" ${col.type === 'TEXT' ? 'selected' : ''}>TEXT</option>
            \\              <option value="INTEGER" ${col.type === 'INTEGER' ? 'selected' : ''}>INTEGER</option>
            \\              <option value="REAL" ${col.type === 'REAL' ? 'selected' : ''}>REAL</option>
            \\              <option value="BOOLEAN" ${col.type === 'BOOLEAN' ? 'selected' : ''}>BOOLEAN</option>
            \\              <option value="TIMESTAMP" ${col.type === 'TIMESTAMP' ? 'selected' : ''}>TIMESTAMP</option>
            \\            </select>
            \\          </td>
            \\          <td style="text-align: center;">
            \\            <input type="checkbox" ${col.is_primary_key ? 'checked' : ''} onchange="onImportColumnPkToggle(${idx}, this.checked)" style="cursor: pointer;">
            \\          </td>
            \\          <td style="text-align: center;">
            \\            <input type="checkbox" ${col.auto_increment ? 'checked' : ''} ${(!col.is_primary_key || col.type !== 'INTEGER') ? 'disabled' : ''} onchange="excelImportColumns[${idx}].auto_increment = this.checked;" style="cursor: pointer;">
            \\          </td>
            \\          <td style="text-align: center;">
            \\            <input type="checkbox" ${col.nullable ? 'checked' : ''} ${col.is_primary_key ? 'disabled' : ''} onchange="excelImportColumns[${idx}].nullable = this.checked;" style="cursor: pointer;">
            \\          </td>
            \\          <td>
            \\            <input type="text" class="form-control" style="padding: 4px 8px; font-size: 12px;" placeholder="Optional field description..." value="${col.description || ''}" onchange="excelImportColumns[${idx}].description = this.value;">
            \\          </td>
            \\        `;
            \\        tbody.appendChild(tr);
            \\      });
            \\    }
            \\
            \\    function renderExcelImportPreviewTable() {
            \\      const thead = document.getElementById('excelImportPreviewThead');
            \\      const tbody = document.getElementById('excelImportPreviewTbody');
            \\      thead.innerHTML = '';
            \\      tbody.innerHTML = '';
            \\
            \\      if (!excelRawData || !excelImportColumns || excelImportColumns.length === 0) return;
            \\
            \\      const headerTr = document.createElement('tr');
            \\      const lineTh = document.createElement('th');
            \\      lineTh.style.width = '160px';
            \\      lineTh.innerHTML = '<div style="font-weight: 700; color: #94a3b8;">Source Line</div><div style="font-size: 10px; color: #64748b;">CSV Line #</div>';
            \\      headerTr.appendChild(lineTh);
            \\
            \\      excelImportColumns.forEach(col => {
            \\        const th = document.createElement('th');
            \\        th.innerHTML = `
            \\          <div style="font-weight: 700; color: ${col.is_primary_key ? '#38bdf8' : '#e2e8f0'}; display: flex; align-items: center; gap: 4px;">
            \\            ${col.is_primary_key ? '🔑 ' : ''}${col.name}
            \\          </div>
            \\          <div style="font-size: 10px; color: #94a3b8; font-weight: normal;">${col.type}</div>
            \\        `;
            \\        headerTr.appendChild(th);
            \\      });
            \\      thead.appendChild(headerTr);
            \\
            \\      const previewRows = excelRawData.rows.slice(0, 5);
            \\      previewRows.forEach((rawRow, rowIdx) => {
            \\        const tr = document.createElement('tr');
            \\        const lineTd = document.createElement('td');
            \\        const csvLineNumber = rowIdx + 2; // Line 1 is headers, Line 2 is first data row
            \\        lineTd.innerHTML = `<span class="badge-type" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; font-family: monospace; font-size: 11px; font-weight: 600;">Line ${csvLineNumber} (Data Row ${rowIdx + 1})</span>`;
            \\        tr.appendChild(lineTd);
            \\
            \\        excelImportColumns.forEach(col => {
            \\          const td = document.createElement('td');
            \\          if (col.isGeneratedPk) {
            \\            td.innerHTML = `<span style="font-weight: 700; color: #38bdf8; font-family: monospace;">${rowIdx + 1}</span>`;
            \\          } else {
            \\            const rawVal = rawRow[col.sourceIdx];
            \\            const displayVal = (rawVal === null || rawVal === undefined || rawVal === '') ? '<span style="color: #64748b; font-style: italic;">NULL</span>' : String(rawVal);
            \\            td.innerHTML = `<span style="font-family: monospace; font-size: 12px;">${displayVal}</span>`;
            \\          }
            \\          tr.appendChild(td);
            \\        });
            \\        tbody.appendChild(tr);
            \\      });
            \\
            \\      document.getElementById('excelImportRowSummary').innerText = 'Data starts on Line 2 • Showing lines 2–' + (previewRows.length + 1) + ' of ' + (excelRawData.rows.length + 1) + ' lines (' + excelRawData.rows.length + ' data rows)';
            \\    }
            \\
            \\    async function submitExcelImport() {
            \\      hideExcelImportAlert();
            \\      const tableName = document.getElementById('excelImportTableName').value.trim();
            \\      const tableDesc = document.getElementById('excelImportTableDesc').value.trim();
            \\
            \\      if (!tableName) {
            \\        showExcelImportAlert('error', 'Please provide a Target Table Name.');
            \\        return;
            \\      }
            \\      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
            \\        showExcelImportAlert('error', 'Table name must start with a letter or underscore and contain only alphanumeric characters and underscores.');
            \\        return;
            \\      }
            \\      if (!excelRawData || !excelRawData.rows || excelRawData.rows.length === 0) {
            \\        showExcelImportAlert('error', 'No rows found to import.');
            \\        return;
            \\      }
            \\
            \\      const pkCols = excelImportColumns.filter(c => c.is_primary_key);
            \\      if (pkCols.length === 0) {
            \\        showExcelImportAlert('error', 'Table must have at least one Primary Key column specified.');
            \\        return;
            \\      }
            \\
            \\      const submitBtn = document.getElementById('excelImportSubmitBtn');
            \\      const statusText = document.getElementById('excelImportStatusText');
            \\      submitBtn.disabled = true;
            \\      submitBtn.innerText = 'Importing...';
            \\      statusText.innerText = 'Writing schema and inserting ' + excelRawData.rows.length + ' rows...';
            \\
            \\      try {
            \\        const rowsPayload = excelRawData.rows.map((rawRow, rowIdx) => {
            \\          return excelImportColumns.map(col => {
            \\            if (col.isGeneratedPk) {
            \\              return String(rowIdx + 1);
            \\            }
            \\            const val = rawRow[col.sourceIdx];
            \\            if (val === null || val === undefined) return null;
            \\            const s = String(val).trim();
            \\            return s.length === 0 ? null : s;
            \\          });
            \\        });
            \\
            \\        const columnsPayload = excelImportColumns.map(col => ({
            \\          name: col.name,
            \\          type: col.type,
            \\          primary_key: col.is_primary_key,
            \\          is_primary_key: col.is_primary_key,
            \\          auto_increment: col.auto_increment,
            \\          nullable: col.nullable,
            \\          description: col.description || ''
            \\        }));
            \\
            \\        const payload = {
            \\          name: tableName,
            \\          description: tableDesc,
            \\          columns: columnsPayload,
            \\          rows: rowsPayload
            \\        };
            \\
            \\        const res = await fetch('/api/import/table', {
            \\          method: 'POST',
            \\          headers: { 'Content-Type': 'application/json' },
            \\          body: JSON.stringify(payload)
            \\        });
            \\
            \\        const data = await res.json();
            \\        if (data.error) {
            \\          showExcelImportAlert('error', 'Import failed: ' + data.error);
            \\          submitBtn.disabled = false;
            \\          submitBtn.innerText = 'Import Table & Data';
            \\          statusText.innerText = '';
            \\          return;
            \\        }
            \\
            \\        closeExcelImportModal();
            \\        await fetchTables();
            \\        openTableInTableData(tableName);
            \\        alert('🎉 Successfully created table "' + tableName + '" and imported ' + (data.imported_rows || rowsPayload.length) + ' records!');
            \\      } catch (err) {
            \\        console.error(err);
            \\        showExcelImportAlert('error', 'Network error during import: ' + err.message);
            \\        submitBtn.disabled = false;
            \\        submitBtn.innerText = 'Import Table & Data';
            \\        statusText.innerText = '';
            \\      }
            \\    }
            \\
            \\    document.addEventListener('keydown', (e) => {
            \\      if (e.key === 'Escape') {
            \\        const modals = ['tableDataDetailModal', 'tableDataNewModal', 'tableDataEditModal', 'tableDataCountModal', 'excelImportModal'];
            \\        for (const m of modals) {
            \\          const el = document.getElementById(m);
            \\          if (el && el.style.display !== 'none') {
            \\            el.style.display = 'none';
            \\            return;
            \\          }
            \\        }
            \\      }
            \\      if (currentMainTab === 'tableData') {
            \\        const gridScreen = document.getElementById('tableDataGridScreen');
            \\        const isGrid = gridScreen && gridScreen.style.display !== 'none';
            \\        if (e.key === 'F8') {
            \\          e.preventDefault();
            \\          if (isGrid) refreshTableDataGrid();
            \\          else executeTableDataQuery();
            \\        } else if (e.key === 'F3') {
            \\          if (isGrid) {
            \\            e.preventDefault();
            \\            backToTableDataSelection();
            \\          }
            \\        } else if (e.key === 'F2') {
            \\          if (isGrid) {
            \\            e.preventDefault();
            \\            openTableDataDetailModal();
            \\          }
            \\        } else if (e.key === 'F10' && e.shiftKey) {
            \\          if (!isGrid) {
            \\            e.preventDefault();
            \\            countTableDataEntries();
            \\          }
            \\        }
            \\      }
            \\    });
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
            const tbl_desc = self.engine.getTableDescription(table.name);

            writer.writeAll("{\"name\":") catch return;
            writeJsonString(writer, table.name) catch return;
            writer.writeAll(",\"description\":") catch return;
            writeJsonString(writer, tbl_desc) catch return;
            writer.print(",\"row_count\":{d}", .{table.rows.items.len}) catch return;

            writer.writeAll(",\"columns\":[") catch return;
            for (table.schema.columns, 0..) |col, c_idx| {
                if (c_idx > 0) writer.writeAll(",") catch return;
                const col_desc = self.engine.getColumnDescription(table.name, col.name);

                writer.writeAll("{\"name\":") catch return;
                writeJsonString(writer, col.name) catch return;
                writer.print(",\"type\":\"{s}\",\"primary_key\":{s},\"auto_increment\":{s},\"nullable\":{s},\"description\":", .{
                    col.data_type.name(),
                    if (col.primary_key) "true" else "false",
                    if (col.auto_increment) "true" else "false",
                    if (col.nullable) "true" else "false",
                }) catch return;
                writeJsonString(writer, col_desc) catch return;
                writer.writeAll("}") catch return;
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

    fn sendSuccess(self: *WebServer, stream: std.net.Stream, msg: []const u8) void {
        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();
        writer.writeAll("{\"status\":\"ok\",\"message\":") catch return;
        writeJsonString(writer, msg) catch return;
        writer.writeAll("}") catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn serveCreateTable(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const ColItem = struct {
            name: []const u8,
            type: []const u8,
            primary_key: bool = false,
            auto_increment: bool = false,
            nullable: bool = true,
            description: ?[]const u8 = null,
        };

        const Payload = struct {
            name: []const u8,
            description: ?[]const u8 = null,
            columns: []ColItem,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for create table");
            return;
        };

        const name = std.mem.trim(u8, parsed.value.name, " \t\r\n");
        if (name.len == 0) {
            sendError(stream, 400, "Table name cannot be empty");
            return;
        }

        for (name) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '_') {
                sendError(stream, 400, "Table name must contain only letters, numbers, and underscores");
                return;
            }
        }

        if (parsed.value.columns.len == 0) {
            sendError(stream, 400, "Table must have at least one column");
            return;
        }

        var cols = std.ArrayList(types.Column).init(arena_alloc);
        var pk_count: usize = 0;
        for (parsed.value.columns) |c| {
            const col_name = std.mem.trim(u8, c.name, " \t\r\n");
            if (col_name.len == 0) {
                sendError(stream, 400, "Column name cannot be empty");
                return;
            }
            for (col_name) |ch| {
                if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
                    sendError(stream, 400, "Column name must contain only letters, numbers, and underscores");
                    return;
                }
            }
            const dt = types.DataType.fromString(c.type) orelse {
                sendError(stream, 400, "Invalid column data type");
                return;
            };
            if (c.primary_key) pk_count += 1;
            cols.append(.{
                .name = col_name,
                .data_type = dt,
                .primary_key = c.primary_key,
                .auto_increment = c.auto_increment,
                .nullable = if (c.primary_key) false else c.nullable,
            }) catch {
                sendError(stream, 500, "Out of memory");
                return;
            };
        }

        if (pk_count != 1) {
            sendError(stream, 400, "Table must specify exactly one PRIMARY KEY column");
            return;
        }

        const schema = types.Schema.init(arena_alloc, cols.items) catch {
            sendError(stream, 500, "Out of memory");
            return;
        };

        _ = self.engine.createTable(name, schema) catch |err| {
            if (err == error.TableAlreadyExists) {
                sendError(stream, 400, "Table already exists");
            } else {
                var err_buf: [128]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "Failed to create table: {s}", .{@errorName(err)}) catch "Failed to create table";
                sendError(stream, 500, msg);
            }
            return;
        };

        if (parsed.value.description) |desc| {
            self.engine.setTableDescription(name, desc) catch {};
        }

        for (parsed.value.columns) |c| {
            if (c.description) |cdesc| {
                const col_name = std.mem.trim(u8, c.name, " \t\r\n");
                self.engine.setColumnDescription(name, col_name, cdesc) catch {};
            }
        }

        var msg_buf: [256]u8 = undefined;
        const success_msg = std.fmt.bufPrint(&msg_buf, "Table '{s}' created successfully with {d} columns.", .{ name, cols.items.len }) catch "Table created successfully";
        self.sendSuccess(stream, success_msg);
    }

    fn serveSaveMetadata(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const ColMeta = struct {
            name: []const u8,
            description: []const u8 = "",
        };

        const Payload = struct {
            table: []const u8,
            description: ?[]const u8 = null,
            columns: ?[]ColMeta = null,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for save metadata");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        if (self.engine.getTable(table_name) == null) {
            sendError(stream, 404, "Table not found");
            return;
        }

        if (parsed.value.description) |desc| {
            self.engine.setTableDescription(table_name, desc) catch |err| {
                sendError(stream, 500, @errorName(err));
                return;
            };
        }

        if (parsed.value.columns) |cols| {
            for (cols) |c| {
                const col_name = std.mem.trim(u8, c.name, " \t\r\n");
                self.engine.setColumnDescription(table_name, col_name, c.description) catch {};
            }
        }

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Metadata for table '{s}' saved successfully.", .{table_name}) catch "Metadata saved successfully";
        self.sendSuccess(stream, msg);
    }

    fn serveAddColumn(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const ColItem = struct {
            name: []const u8,
            type: []const u8,
            primary_key: bool = false,
            auto_increment: bool = false,
            nullable: bool = true,
            description: ?[]const u8 = null,
        };

        const Payload = struct {
            table: []const u8,
            column: ColItem,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for add column");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        const col_name = std.mem.trim(u8, parsed.value.column.name, " \t\r\n");
        if (col_name.len == 0) {
            sendError(stream, 400, "Column name cannot be empty");
            return;
        }
        for (col_name) |ch| {
            if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
                sendError(stream, 400, "Column name must contain only letters, numbers, and underscores");
                return;
            }
        }

        const dt = types.DataType.fromString(parsed.value.column.type) orelse {
            sendError(stream, 400, "Invalid column data type");
            return;
        };

        const col = types.Column{
            .name = col_name,
            .data_type = dt,
            .primary_key = parsed.value.column.primary_key,
            .auto_increment = parsed.value.column.auto_increment,
            .nullable = if (parsed.value.column.primary_key) false else parsed.value.column.nullable,
        };

        self.engine.alterTableAddColumn(table_name, col) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to add column: {s}", .{@errorName(err)}) catch "Failed to add column";
            sendError(stream, 400, msg);
            return;
        };

        if (parsed.value.column.description) |desc| {
            self.engine.setColumnDescription(table_name, col_name, desc) catch {};
        }

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Column '{s}' added to table '{s}'.", .{ col_name, table_name }) catch "Column added";
        self.sendSuccess(stream, msg);
    }

    fn serveModifyColumn(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const ColItem = struct {
            name: []const u8,
            type: []const u8,
            primary_key: ?bool = null,
            auto_increment: ?bool = null,
            nullable: ?bool = null,
            description: ?[]const u8 = null,
        };

        const Payload = struct {
            table: []const u8,
            column: ColItem,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for modify column");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        const col_name = std.mem.trim(u8, parsed.value.column.name, " \t\r\n");

        const dt = types.DataType.fromString(parsed.value.column.type) orelse {
            sendError(stream, 400, "Invalid column data type");
            return;
        };

        const table = self.engine.getTable(table_name) orelse {
            sendError(stream, 404, "Table not found");
            return;
        };
        const col_idx = table.schema.getColumnIndex(col_name) orelse {
            sendError(stream, 404, "Column not found");
            return;
        };
        const current_col = table.schema.columns[col_idx];

        const is_pk = parsed.value.column.primary_key orelse current_col.primary_key;
        const col = types.Column{
            .name = col_name,
            .data_type = dt,
            .primary_key = is_pk,
            .auto_increment = parsed.value.column.auto_increment orelse current_col.auto_increment,
            .nullable = if (is_pk) false else (parsed.value.column.nullable orelse current_col.nullable),
        };

        self.engine.alterTableModifyColumn(table_name, col) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to modify column: {s}", .{@errorName(err)}) catch "Failed to modify column";
            sendError(stream, 400, msg);
            return;
        };

        if (parsed.value.column.description) |desc| {
            self.engine.setColumnDescription(table_name, col_name, desc) catch {};
        }

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Column '{s}' modified in table '{s}'.", .{ col_name, table_name }) catch "Column modified";
        self.sendSuccess(stream, msg);
    }

    fn serveRenameColumn(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const Payload = struct {
            table: []const u8,
            old_name: []const u8,
            new_name: []const u8,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for rename column");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        const old_name = std.mem.trim(u8, parsed.value.old_name, " \t\r\n");
        const new_name = std.mem.trim(u8, parsed.value.new_name, " \t\r\n");

        if (old_name.len == 0 or new_name.len == 0) {
            sendError(stream, 400, "Column names cannot be empty");
            return;
        }

        for (new_name) |ch| {
            if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
                sendError(stream, 400, "New column name must contain only letters, numbers, and underscores");
                return;
            }
        }

        self.engine.alterTableRenameColumn(table_name, old_name, new_name) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to rename column: {s}", .{@errorName(err)}) catch "Failed to rename column";
            sendError(stream, 400, msg);
            return;
        };

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Column '{s}' renamed to '{s}' in table '{s}'.", .{ old_name, new_name, table_name }) catch "Column renamed";
        self.sendSuccess(stream, msg);
    }

    fn serveDropColumn(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const Payload = struct {
            table: []const u8,
            column: []const u8,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for drop column");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        const col_name = std.mem.trim(u8, parsed.value.column, " \t\r\n");

        self.engine.alterTableDropColumn(table_name, col_name) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to drop column: {s}", .{@errorName(err)}) catch "Failed to drop column";
            sendError(stream, 400, msg);
            return;
        };

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Column '{s}' dropped from table '{s}'.", .{ col_name, table_name }) catch "Column dropped";
        self.sendSuccess(stream, msg);
    }

    fn serveDropTable(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const Payload = struct {
            table: []const u8,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for drop table");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");

        const dropped = self.engine.dropTable(table_name) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to drop table: {s}", .{@errorName(err)}) catch "Failed to drop table";
            sendError(stream, 500, msg);
            return;
        };

        if (!dropped) {
            sendError(stream, 404, "Table not found");
            return;
        }

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Table '{s}' dropped successfully.", .{table_name}) catch "Table dropped";
        self.sendSuccess(stream, msg);
    }

    fn serveSetPrimaryKey(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const Payload = struct {
            table: []const u8,
            column: []const u8,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for set primary key");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        const col_name = std.mem.trim(u8, parsed.value.column, " \t\r\n");

        if (table_name.len == 0 or col_name.len == 0) {
            sendError(stream, 400, "Table name and column name cannot be empty");
            return;
        }

        self.engine.alterTableRedefinePrimaryKey(table_name, col_name) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to set primary key: {s}", .{@errorName(err)}) catch "Failed to set primary key";
            sendError(stream, 400, msg);
            return;
        };

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Primary key for table '{s}' set to column '{s}'.", .{ table_name, col_name }) catch "Primary key updated";
        self.sendSuccess(stream, msg);
    }

    fn serveDropPrimaryKey(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const Payload = struct {
            table: []const u8,
        };

        const parsed = std.json.parseFromSlice(Payload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for drop primary key");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");

        if (table_name.len == 0) {
            sendError(stream, 400, "Table name cannot be empty");
            return;
        }

        self.engine.alterTableDropPrimaryKey(table_name) catch |err| {
            var err_buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to drop primary key: {s}", .{@errorName(err)}) catch "Failed to drop primary key";
            sendError(stream, 400, msg);
            return;
        };

        var msg_buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "Primary key dropped for table '{s}'.", .{table_name}) catch "Primary key dropped";
        self.sendSuccess(stream, msg);
    }

    fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
        if (needle.len == 0) return true;
        if (haystack.len < needle.len) return false;
        var i: usize = 0;
        while (i + needle.len <= haystack.len) : (i += 1) {
            if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
        }
        return false;
    }

    fn startsWithIgnoreCase(haystack: []const u8, needle: []const u8) bool {
        if (haystack.len < needle.len) return false;
        return std.ascii.eqlIgnoreCase(haystack[0..needle.len], needle);
    }

    fn endsWithIgnoreCase(haystack: []const u8, needle: []const u8) bool {
        if (haystack.len < needle.len) return false;
        return std.ascii.eqlIgnoreCase(haystack[haystack.len - needle.len ..], needle);
    }

    fn parseFieldValue(val_str_opt: ?[]const u8, col: types.Column, allocator: std.mem.Allocator) !Value {
        if (val_str_opt == null) {
            if (!col.nullable and !col.auto_increment) {
                return error.NotNullViolation;
            }
            return Value{ .null_val = {} };
        }
        const val_str = val_str_opt.?;
        if (std.ascii.eqlIgnoreCase(val_str, "NULL")) {
            if (!col.nullable and !col.auto_increment) {
                return error.NotNullViolation;
            }
            return Value{ .null_val = {} };
        }

        return switch (col.data_type) {
            .integer => {
                const trimmed = std.mem.trim(u8, val_str, " \t\r\n");
                if (trimmed.len == 0) {
                    if (!col.nullable and !col.auto_increment) return error.NotNullViolation;
                    return Value{ .null_val = {} };
                }
                const int_val = std.fmt.parseInt(i64, trimmed, 10) catch return error.InvalidInteger;
                return Value{ .integer = int_val };
            },
            .real => {
                const trimmed = std.mem.trim(u8, val_str, " \t\r\n");
                if (trimmed.len == 0) {
                    if (!col.nullable) return error.NotNullViolation;
                    return Value{ .null_val = {} };
                }
                const float_val = std.fmt.parseFloat(f64, trimmed) catch return error.InvalidReal;
                return Value{ .real = float_val };
            },
            .text => {
                return Value{ .text = try allocator.dupe(u8, val_str) };
            },
            .boolean => {
                const trimmed = std.mem.trim(u8, val_str, " \t\r\n");
                if (trimmed.len == 0) {
                    if (!col.nullable) return error.NotNullViolation;
                    return Value{ .null_val = {} };
                }
                if (std.ascii.eqlIgnoreCase(trimmed, "true") or
                    std.ascii.eqlIgnoreCase(trimmed, "1") or
                    std.ascii.eqlIgnoreCase(trimmed, "t") or
                    std.ascii.eqlIgnoreCase(trimmed, "yes") or
                    std.ascii.eqlIgnoreCase(trimmed, "x"))
                {
                    return Value{ .boolean = true };
                } else {
                    return Value{ .boolean = false };
                }
            },
            .timestamp => {
                const trimmed = std.mem.trim(u8, val_str, " \t\r\n");
                if (trimmed.len == 0) {
                    if (!col.nullable) return error.NotNullViolation;
                    return Value{ .null_val = {} };
                }
                const ts_val = std.fmt.parseInt(i64, trimmed, 10) catch {
                    return Value{ .timestamp = std.time.milliTimestamp() };
                };
                return Value{ .timestamp = ts_val };
            },
            .blob => {
                return Value{ .blob = try allocator.dupe(u8, val_str) };
            },
        };
    }

    fn tableDataMatchesFilter(row: Row, table: *Table, flt: anytype, allocator: std.mem.Allocator) bool {
        var col_idx_opt: ?usize = null;
        for (table.schema.columns, 0..) |c, i| {
            if (std.ascii.eqlIgnoreCase(c.name, flt.column)) {
                col_idx_opt = i;
                break;
            }
        }
        const col_idx = col_idx_opt orelse return true;
        if (col_idx >= row.values.len) return true;
        const cell_val = row.values[col_idx];
        const col_def = table.schema.columns[col_idx];

        const op = std.mem.trim(u8, flt.op, " \t\r\n");
        if (std.ascii.eqlIgnoreCase(op, "IS NULL")) {
            return cell_val == .null_val;
        }
        if (std.ascii.eqlIgnoreCase(op, "IS NOT NULL")) {
            return cell_val != .null_val;
        }

        const low_trimmed = std.mem.trim(u8, flt.low, " \t\r\n");
        if (low_trimmed.len == 0) return true;

        if (cell_val == .null_val) return false;

        if (std.ascii.eqlIgnoreCase(op, "LIKE") or std.ascii.eqlIgnoreCase(op, "CP")) {
            const cell_str = cell_val.toStringAlloc(allocator) catch return false;
            defer allocator.free(cell_str);

            var pat = low_trimmed;
            const starts_wild = std.mem.startsWith(u8, pat, "%") or std.mem.startsWith(u8, pat, "*");
            const ends_wild = std.mem.endsWith(u8, pat, "%") or std.mem.endsWith(u8, pat, "*");

            if (starts_wild and ends_wild and pat.len >= 2) {
                pat = pat[1 .. pat.len - 1];
                return containsIgnoreCase(cell_str, pat);
            } else if (starts_wild and pat.len >= 1) {
                pat = pat[1..];
                return endsWithIgnoreCase(cell_str, pat);
            } else if (ends_wild and pat.len >= 1) {
                pat = pat[0 .. pat.len - 1];
                return startsWithIgnoreCase(cell_str, pat);
            } else {
                return containsIgnoreCase(cell_str, pat);
            }
        }

        const low_val = parseFieldValue(low_trimmed, col_def, allocator) catch return false;
        defer low_val.deinit(allocator);

        if (std.ascii.eqlIgnoreCase(op, "=") or std.ascii.eqlIgnoreCase(op, "EQ")) {
            return cell_val.isEqual(low_val);
        }
        if (std.ascii.eqlIgnoreCase(op, "<>") or std.ascii.eqlIgnoreCase(op, "!=") or std.ascii.eqlIgnoreCase(op, "NE")) {
            return !cell_val.isEqual(low_val);
        }
        if (std.ascii.eqlIgnoreCase(op, ">") or std.ascii.eqlIgnoreCase(op, "GT")) {
            return cell_val.compare(low_val) == .gt;
        }
        if (std.ascii.eqlIgnoreCase(op, ">=") or std.ascii.eqlIgnoreCase(op, "GE")) {
            const ord = cell_val.compare(low_val);
            return ord == .gt or ord == .eq;
        }
        if (std.ascii.eqlIgnoreCase(op, "<") or std.ascii.eqlIgnoreCase(op, "LT")) {
            return cell_val.compare(low_val) == .lt;
        }
        if (std.ascii.eqlIgnoreCase(op, "<=") or std.ascii.eqlIgnoreCase(op, "LE")) {
            const ord = cell_val.compare(low_val);
            return ord == .lt or ord == .eq;
        }
        if (std.ascii.eqlIgnoreCase(op, "BETWEEN") or std.ascii.eqlIgnoreCase(op, "BT")) {
            const high_trimmed = std.mem.trim(u8, flt.high, " \t\r\n");
            if (high_trimmed.len == 0) return true;
            const high_val = parseFieldValue(high_trimmed, col_def, allocator) catch return false;
            defer high_val.deinit(allocator);

            return (cell_val.compare(low_val) != .lt and cell_val.compare(high_val) != .gt);
        }

        return true;
    }

    fn serveTableDataQuery(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const TableDataFilterItem = struct {
            column: []const u8,
            op: []const u8 = "=",
            low: []const u8 = "",
            high: []const u8 = "",
        };
        const TableDataQueryPayload = struct {
            table: []const u8,
            max_rows: ?usize = 500,
            columns: ?[][]const u8 = null,
            filters: ?[]TableDataFilterItem = null,
            sort_by: ?[]const u8 = null,
            sort_dir: ?[]const u8 = null,
        };

        const parsed = std.json.parseFromSlice(TableDataQueryPayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for TableData query");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        self.engine.mutex.lock();
        const table = self.engine.tables.get(table_name) orelse {
            self.engine.mutex.unlock();
            sendError(stream, 404, "Table not found");
            return;
        };

        table.mutex.lock();
        defer table.mutex.unlock();
        defer self.engine.mutex.unlock();

        var matched_indices = std.ArrayList(usize).init(arena_alloc);
        for (table.rows.items, 0..) |row, idx| {
            var matches = true;
            if (parsed.value.filters) |filters| {
                for (filters) |flt| {
                    if (!tableDataMatchesFilter(row, table, flt, arena_alloc)) {
                        matches = false;
                        break;
                    }
                }
            }
            if (matches) {
                matched_indices.append(idx) catch {};
            }
        }

        const total_hits = matched_indices.items.len;

        if (parsed.value.sort_by) |sb| {
            if (sb.len > 0) {
                var sort_col_idx: ?usize = null;
                for (table.schema.columns, 0..) |col, c_idx| {
                    if (std.ascii.eqlIgnoreCase(col.name, sb)) {
                        sort_col_idx = c_idx;
                        break;
                    }
                }
                if (sort_col_idx) |sc_idx| {
                    const SortCtx = struct {
                        tbl: *Table,
                        col_idx: usize,
                        asc: bool,
                        pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                            const val_a = ctx.tbl.rows.items[a].values[ctx.col_idx];
                            const val_b = ctx.tbl.rows.items[b].values[ctx.col_idx];
                            const ord = val_a.compare(val_b);
                            return if (ctx.asc) ord == .lt else ord == .gt;
                        }
                    };
                    const asc = !std.ascii.eqlIgnoreCase(parsed.value.sort_dir orelse "ASC", "DESC");
                    std.mem.sort(usize, matched_indices.items, SortCtx{ .tbl = table, .col_idx = sc_idx, .asc = asc }, SortCtx.lessThan);
                }
            }
        }

        var output_cols = std.ArrayList(usize).init(arena_alloc);
        if (parsed.value.columns) |req_cols| {
            if (req_cols.len > 0) {
                for (req_cols) |rc| {
                    for (table.schema.columns, 0..) |col, c_idx| {
                        if (std.ascii.eqlIgnoreCase(col.name, rc)) {
                            output_cols.append(c_idx) catch {};
                            break;
                        }
                    }
                }
            }
        }
        if (output_cols.items.len == 0) {
            for (table.schema.columns, 0..) |_, c_idx| {
                output_cols.append(c_idx) catch {};
            }
        }

        const max_rows = parsed.value.max_rows orelse 500;
        const limit = if (max_rows == 0) total_hits else @min(total_hits, max_rows);

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();

        writer.print("{{\"status\":\"ok\",\"table\":", .{}) catch return;
        writeJsonString(writer, table_name) catch return;
        writer.print(",\"total_hits\":{d},\"returned_rows\":{d},\"columns\":[", .{ total_hits, limit }) catch return;

        for (output_cols.items, 0..) |c_idx, i| {
            if (i > 0) writer.writeAll(",") catch return;
            const col = table.schema.columns[c_idx];
            const col_desc = self.engine.getColumnDescription(table_name, col.name);
            writer.writeAll("{\"name\":") catch return;
            writeJsonString(writer, col.name) catch return;
            writer.print(",\"type\":\"{s}\",\"primary_key\":{s},\"auto_increment\":{s},\"nullable\":{s},\"description\":", .{
                col.data_type.name(),
                if (col.primary_key) "true" else "false",
                if (col.auto_increment) "true" else "false",
                if (col.nullable) "true" else "false",
            }) catch return;
            writeJsonString(writer, col_desc) catch return;
            writer.writeAll("}") catch return;
        }

        writer.writeAll("],\"row_indices\":[") catch return;
        for (0..limit) |i| {
            if (i > 0) writer.writeAll(",") catch return;
            writer.print("{d}", .{matched_indices.items[i]}) catch return;
        }

        writer.writeAll("],\"rows\":[") catch return;
        for (0..limit) |i| {
            if (i > 0) writer.writeAll(",") catch return;
            const row_idx = matched_indices.items[i];
            const row = table.rows.items[row_idx];

            writer.writeAll("[") catch return;
            for (output_cols.items, 0..) |c_idx, j| {
                if (j > 0) writer.writeAll(",") catch return;
                if (c_idx < row.values.len) {
                    const val = row.values[c_idx];
                    switch (val) {
                        .null_val => writer.writeAll("null") catch return,
                        .integer => |iv| writer.print("{d}", .{iv}) catch return,
                        .real => |rv| writer.print("{d:.4}", .{rv}) catch return,
                        .boolean => |bv| writer.writeAll(if (bv) "true" else "false") catch return,
                        .timestamp => |tv| writer.print("{d}", .{tv}) catch return,
                        .text => |tv| writeJsonString(writer, tv) catch return,
                        .blob => |bv| writeJsonString(writer, bv) catch return,
                    }
                } else {
                    writer.writeAll("null") catch return;
                }
            }
            writer.writeAll("]") catch return;
        }
        writer.writeAll("]}") catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn serveTableDataCount(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const TableDataFilterItem = struct {
            column: []const u8,
            op: []const u8 = "=",
            low: []const u8 = "",
            high: []const u8 = "",
        };
        const TableDataCountPayload = struct {
            table: []const u8,
            filters: ?[]TableDataFilterItem = null,
        };

        const parsed = std.json.parseFromSlice(TableDataCountPayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for TableData count");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        self.engine.mutex.lock();
        const table = self.engine.tables.get(table_name) orelse {
            self.engine.mutex.unlock();
            sendError(stream, 404, "Table not found");
            return;
        };

        table.mutex.lock();
        var match_count: usize = 0;
        for (table.rows.items) |row| {
            var matches = true;
            if (parsed.value.filters) |filters| {
                for (filters) |flt| {
                    if (!tableDataMatchesFilter(row, table, flt, arena_alloc)) {
                        matches = false;
                        break;
                    }
                }
            }
            if (matches) match_count += 1;
        }
        table.mutex.unlock();
        self.engine.mutex.unlock();

        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();
        writer.print("{{\"status\":\"ok\",\"table\":", .{}) catch return;
        writeJsonString(writer, table_name) catch return;
        writer.print(",\"count\":{d}}}", .{match_count}) catch return;

        const swriter = stream.writer();
        swriter.print(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ json_buf.items.len, json_buf.items },
        ) catch {};
    }

    fn serveTableDataInsert(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const TableDataFieldValue = struct {
            column: []const u8,
            value: ?[]const u8 = null,
        };
        const TableDataInsertPayload = struct {
            table: []const u8,
            fields: []TableDataFieldValue,
        };

        const parsed = std.json.parseFromSlice(TableDataInsertPayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for TableData insert");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        self.engine.mutex.lock();
        const table = self.engine.tables.get(table_name) orelse {
            self.engine.mutex.unlock();
            sendError(stream, 404, "Table not found");
            return;
        };
        const schema_cols = table.schema.columns;
        self.engine.mutex.unlock();

        var values = arena_alloc.alloc(Value, schema_cols.len) catch {
            sendError(stream, 500, "Out of memory");
            return;
        };

        for (schema_cols, 0..) |col, i| {
            var found_val: ?[]const u8 = null;
            for (parsed.value.fields) |f| {
                if (std.ascii.eqlIgnoreCase(f.column, col.name)) {
                    found_val = f.value;
                    break;
                }
            }
            values[i] = parseFieldValue(found_val, col, arena_alloc) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "Validation error on column '{s}': {s}", .{ col.name, @errorName(err) }) catch "Validation error";
                sendError(stream, 400, msg);
                return;
            };
        }

        const new_row = Row{ .values = values };
        self.engine.insertRow(table_name, new_row) catch |err| {
            var err_buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&err_buf, "Failed to insert row: {s}", .{@errorName(err)}) catch "Failed to insert row";
            sendError(stream, 400, msg);
            return;
        };

        self.sendSuccess(stream, "1 row successfully inserted.");
    }

    fn serveTableDataUpdate(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const TableDataFieldValue = struct {
            column: []const u8,
            value: ?[]const u8 = null,
        };
        const TableDataUpdatePayload = struct {
            table: []const u8,
            row_index: ?usize = null,
            pk_value: ?[]const u8 = null,
            fields: []TableDataFieldValue,
        };

        const parsed = std.json.parseFromSlice(TableDataUpdatePayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for TableData update");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        self.engine.mutex.lock();
        const table = self.engine.tables.get(table_name) orelse {
            self.engine.mutex.unlock();
            sendError(stream, 404, "Table not found");
            return;
        };
        const schema_cols = table.schema.columns;
        var existing_row_opt: ?Row = null;
        if (parsed.value.row_index) |idx| {
            if (idx < table.rows.items.len) {
                existing_row_opt = table.rows.items[idx];
            }
        }
        self.engine.mutex.unlock();

        var values = arena_alloc.alloc(Value, schema_cols.len) catch {
            sendError(stream, 500, "Out of memory");
            return;
        };

        for (schema_cols, 0..) |col, i| {
            var found_val: ?[]const u8 = null;
            var is_provided = false;
            for (parsed.value.fields) |f| {
                if (std.ascii.eqlIgnoreCase(f.column, col.name)) {
                    found_val = f.value;
                    is_provided = true;
                    break;
                }
            }
            if (!is_provided and existing_row_opt != null and i < existing_row_opt.?.values.len) {
                values[i] = existing_row_opt.?.values[i].clone(arena_alloc) catch {
                    sendError(stream, 500, "Out of memory");
                    return;
                };
            } else {
                values[i] = parseFieldValue(found_val, col, arena_alloc) catch |err| {
                    var err_buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrint(&err_buf, "Validation error on column '{s}': {s}", .{ col.name, @errorName(err) }) catch "Validation error";
                    sendError(stream, 400, msg);
                    return;
                };
            }
        }

        const new_row = Row{ .values = values };

        if (parsed.value.row_index) |idx| {
            self.engine.updateRowByIndex(table_name, idx, new_row) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "Failed to update row at index {d}: {s}", .{ idx, @errorName(err) }) catch "Failed to update row";
                sendError(stream, 400, msg);
                return;
            };
        } else if (parsed.value.pk_value) |pk_str| {
            if (table.schema.getPrimaryKeyIndex()) |pk_idx| {
                const pk_col = table.schema.columns[pk_idx];
                const pk_val = parseFieldValue(pk_str, pk_col, arena_alloc) catch {
                    sendError(stream, 400, "Invalid primary key value");
                    return;
                };
                self.engine.updateRow(table_name, pk_val, new_row) catch |err| {
                    var err_buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrint(&err_buf, "Failed to update row by PK: {s}", .{@errorName(err)}) catch "Failed to update row";
                    sendError(stream, 400, msg);
                    return;
                };
            } else {
                sendError(stream, 400, "Table has no primary key and no row_index provided");
                return;
            }
        } else {
            sendError(stream, 400, "Must provide row_index or pk_value to update");
            return;
        }

        self.sendSuccess(stream, "1 row successfully updated.");
    }

    fn serveTableDataDelete(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const TableDataDeletePayload = struct {
            table: []const u8,
            row_indices: ?[]usize = null,
            pk_values: ?[][]const u8 = null,
        };

        const parsed = std.json.parseFromSlice(TableDataDeletePayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for TableData delete");
            return;
        };

        const table_name = std.mem.trim(u8, parsed.value.table, " \t\r\n");
        self.engine.mutex.lock();
        const table = self.engine.tables.get(table_name) orelse {
            self.engine.mutex.unlock();
            sendError(stream, 404, "Table not found");
            return;
        };
        const pk_idx_opt = table.schema.getPrimaryKeyIndex();
        self.engine.mutex.unlock();

        var deleted_count: usize = 0;

        if (pk_idx_opt) |pk_idx| {
            if (parsed.value.pk_values) |pks| {
                const pk_col = table.schema.columns[pk_idx];
                for (pks) |pk_str| {
                    if (parseFieldValue(pk_str, pk_col, arena_alloc)) |pk_val| {
                        if (self.engine.deleteRow(table_name, pk_val) catch false) {
                            deleted_count += 1;
                        }
                    } else |_| {}
                }
                var msg_buf: [128]u8 = undefined;
                const msg = std.fmt.bufPrint(&msg_buf, "{d} row(s) deleted successfully.", .{deleted_count}) catch "Rows deleted";
                self.sendSuccess(stream, msg);
                return;
            }
        }

        if (parsed.value.row_indices) |indices| {
            if (indices.len > 0) {
                const dup_indices = arena_alloc.dupe(usize, indices) catch {
                    sendError(stream, 500, "Out of memory");
                    return;
                };
                std.mem.sort(usize, dup_indices, {}, std.sort.desc(usize));

                for (dup_indices) |idx| {
                    if (self.engine.deleteRowByIndex(table_name, idx) catch false) {
                        deleted_count += 1;
                    }
                }
            }
        }

        var msg_buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "{d} row(s) deleted successfully.", .{deleted_count}) catch "Rows deleted";
        self.sendSuccess(stream, msg);
    }

    fn serveImportTable(self: *WebServer, stream: std.net.Stream, body: []const u8) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const ColItem = struct {
            name: []const u8,
            type: []const u8,
            primary_key: bool = false,
            is_primary_key: ?bool = null,
            auto_increment: bool = false,
            nullable: bool = true,
            description: ?[]const u8 = null,
        };

        const ImportTablePayload = struct {
            name: []const u8,
            description: ?[]const u8 = null,
            columns: []ColItem,
            rows: [][]?[]const u8,
        };

        const parsed = std.json.parseFromSlice(ImportTablePayload, arena_alloc, body, .{ .ignore_unknown_fields = true }) catch {
            sendError(stream, 400, "Invalid JSON payload for table import");
            return;
        };

        const name = std.mem.trim(u8, parsed.value.name, " \t\r\n");
        if (name.len == 0) {
            sendError(stream, 400, "Table name cannot be empty");
            return;
        }

        for (name) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '_') {
                sendError(stream, 400, "Table name must contain only letters, numbers, and underscores");
                return;
            }
        }

        if (parsed.value.columns.len == 0) {
            sendError(stream, 400, "Table must have at least one column");
            return;
        }

        var cols = std.ArrayList(types.Column).init(arena_alloc);
        var pk_count: usize = 0;
        for (parsed.value.columns) |c| {
            const col_name = std.mem.trim(u8, c.name, " \t\r\n");
            if (col_name.len == 0) {
                sendError(stream, 400, "Column name cannot be empty");
                return;
            }
            for (col_name) |ch| {
                if (!std.ascii.isAlphanumeric(ch) and ch != '_') {
                    sendError(stream, 400, "Column name must contain only letters, numbers, and underscores");
                    return;
                }
            }
            const dt = types.DataType.fromString(c.type) orelse {
                sendError(stream, 400, "Invalid column data type");
                return;
            };
            const is_pk = c.primary_key or (c.is_primary_key orelse false);
            if (is_pk) pk_count += 1;
            cols.append(.{
                .name = col_name,
                .data_type = dt,
                .primary_key = is_pk,
                .auto_increment = c.auto_increment,
                .nullable = if (is_pk) false else c.nullable,
            }) catch {
                sendError(stream, 500, "Out of memory");
                return;
            };
        }

        // If no primary key was specified, enforce the default 'id' primary key field
        var generated_id_column = false;
        if (pk_count == 0) {
            var found_id = false;
            for (cols.items) |*c| {
                if (std.ascii.eqlIgnoreCase(c.name, "id")) {
                    c.primary_key = true;
                    c.nullable = false;
                    pk_count = 1;
                    found_id = true;
                    break;
                }
            }
            if (!found_id) {
                cols.insert(0, .{
                    .name = "id",
                    .data_type = .integer,
                    .primary_key = true,
                    .auto_increment = true,
                    .nullable = false,
                }) catch {
                    sendError(stream, 500, "Out of memory");
                    return;
                };
                pk_count = 1;
                generated_id_column = true;
            }
        }

        if (pk_count < 1) {
            sendError(stream, 400, "Table must specify at least one PRIMARY KEY column");
            return;
        }

        const schema = types.Schema.init(arena_alloc, cols.items) catch {
            sendError(stream, 500, "Out of memory");
            return;
        };

        // Pre-validate all rows before creating the table
        var validated_rows = std.ArrayList([]Value).init(arena_alloc);
        for (parsed.value.rows, 0..) |raw_row, r_idx| {
            var row_values = arena_alloc.alloc(Value, cols.items.len) catch {
                sendError(stream, 500, "Out of memory");
                return;
            };

            const col_offset: usize = if (generated_id_column) 1 else 0;
            if (generated_id_column) {
                row_values[0] = Value{ .integer = @as(i64, @intCast(r_idx + 1)) };
            }

            for (cols.items[col_offset..], 0..) |col, c_idx| {
                const target_idx = c_idx + col_offset;
                const cell_opt = if (c_idx < raw_row.len) raw_row[c_idx] else null;
                row_values[target_idx] = parseFieldValue(cell_opt, col, arena_alloc) catch |err| {
                    var err_buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrint(&err_buf, "Validation error at row {d}, column '{s}': {s}", .{ r_idx + 1, col.name, @errorName(err) }) catch "Validation error in row";
                    sendError(stream, 400, msg);
                    return;
                };
            }
            validated_rows.append(row_values) catch {
                sendError(stream, 500, "Out of memory");
                return;
            };
        }

        // Create table
        const table = self.engine.createTable(name, schema) catch |err| {
            if (err == error.TableAlreadyExists) {
                sendError(stream, 400, "Table already exists");
            } else {
                var err_buf: [128]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "Failed to create table: {s}", .{@errorName(err)}) catch "Failed to create table";
                sendError(stream, 500, msg);
            }
            return;
        };

        // Descriptions
        if (parsed.value.description) |desc| {
            self.engine.setTableDescription(name, desc) catch {};
        }

        for (parsed.value.columns) |c| {
            if (c.description) |cdesc| {
                const col_name = std.mem.trim(u8, c.name, " \t\r\n");
                self.engine.setColumnDescription(name, col_name, cdesc) catch {};
            }
        }
        if (generated_id_column) {
            self.engine.setColumnDescription(name, "id", "Auto-generated primary key") catch {};
        }

        // Insert all rows
        var imported_count: usize = 0;
        for (validated_rows.items) |row_vals| {
            const row = Row{ .values = row_vals };
            self.engine.insertRow(name, row) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "Failed to insert row {d}: {s}", .{ imported_count + 1, @errorName(err) }) catch "Failed to insert row";
                sendError(stream, 500, msg);
                return;
            };
            imported_count += 1;
        }

        // Persist table to disk
        const file_name = std.fmt.allocPrint(arena_alloc, "{s}/{s}.sdb", .{ self.engine.data_dir, name }) catch null;
        if (file_name) |fn_str| {
            table.saveToFile(fn_str) catch {};
        }

        // Response
        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();
        const writer = json_buf.writer();
        writer.print("{{\"status\":\"ok\",\"table\":", .{}) catch return;
        writeJsonString(writer, name) catch return;
        writer.print(",\"imported_rows\":{d},\"columns\":{d},\"message\":", .{ imported_count, cols.items.len }) catch return;
        var msg_buf: [256]u8 = undefined;
        const resp_msg = std.fmt.bufPrint(&msg_buf, "Table '{s}' created successfully with {d} rows and {d} columns.", .{ name, imported_count, cols.items.len }) catch "Table imported successfully";
        writeJsonString(writer, resp_msg) catch return;
        writer.writeAll("}") catch return;

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

        const trimmed_q = std.mem.trim(u8, query_str.?, " \t\r\n;");
        var lower_buf: [32]u8 = undefined;
        const pfx_len = @min(trimmed_q.len, lower_buf.len);
        const pfx = std.ascii.lowerString(&lower_buf, trimmed_q[0..pfx_len]);

        if (std.mem.startsWith(u8, pfx, "import mariadb") or std.mem.startsWith(u8, pfx, "import ")) {
            var path_start: ?usize = null;
            var quote_char: ?u8 = null;
            for (trimmed_q, 0..) |c, i| {
                if (c == '\'' or c == '"') {
                    if (path_start == null) {
                        path_start = i + 1;
                        quote_char = c;
                    } else if (c == quote_char.?) {
                        const file_path = trimmed_q[path_start.?..i];
                        var importer = MariaDbImporter.init(self.allocator, self.engine);
                        const res = importer.importFile(file_path) catch |err| {
                            var err_buf: [256]u8 = undefined;
                            const msg = std.fmt.bufPrint(&err_buf, "MariaDB import failed: {s}", .{@errorName(err)}) catch "MariaDB import failed";
                            sendError(stream, 400, msg);
                            return;
                        };

                        var json_buf = std.ArrayList(u8).init(self.allocator);
                        defer json_buf.deinit();
                        const writer = json_buf.writer();
                        var msg_buf: [256]u8 = undefined;
                        const ok_msg = std.fmt.bufPrint(&msg_buf, "MariaDB 11.3 Import Succeeded! Tables Created: {d}, Rows Inserted: {d}, Statements Processed: {d}, Skipped: {d}, Errors: {d}", .{
                            res.tables_created,
                            res.rows_inserted,
                            res.statements_processed,
                            res.statements_skipped,
                            res.errors,
                        }) catch "MariaDB import completed.";
                        writer.print("{{\"columns\":[],\"rows\":[],\"message\":", .{}) catch return;
                        writeJsonString(writer, ok_msg) catch return;
                        writer.writeAll("}") catch return;

                        const swriter = stream.writer();
                        swriter.print(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
                            .{ json_buf.items.len, json_buf.items },
                        ) catch {};
                        return;
                    }
                }
            }
            sendError(stream, 400, "Usage: IMPORT MARIADB '<filepath.sql>';");
            return;
        } else if (std.mem.startsWith(u8, pfx, "/*!") or std.mem.startsWith(u8, pfx, "-- mariadb") or std.mem.startsWith(u8, pfx, "/*m!")) {
            var importer = MariaDbImporter.init(self.allocator, self.engine);
            const res = importer.importString(query_str.?) catch |err| {
                var err_buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&err_buf, "MariaDB dump ingestion error: {s}", .{@errorName(err)}) catch "MariaDB dump ingestion error";
                sendError(stream, 400, msg);
                return;
            };

            var json_buf = std.ArrayList(u8).init(self.allocator);
            defer json_buf.deinit();
            const writer = json_buf.writer();
            var msg_buf: [256]u8 = undefined;
            const ok_msg = std.fmt.bufPrint(&msg_buf, "MariaDB Raw Dump Ingested: {d} tables created, {d} rows inserted ({d} statements processed, {d} skipped).", .{
                res.tables_created,
                res.rows_inserted,
                res.statements_processed,
                res.statements_skipped,
            }) catch "MariaDB dump completed.";
            writer.print("{{\"columns\":[],\"rows\":[],\"message\":", .{}) catch return;
            writeJsonString(writer, ok_msg) catch return;
            writer.writeAll("}") catch return;

            const swriter = stream.writer();
            swriter.print(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
                .{ json_buf.items.len, json_buf.items },
            ) catch {};
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

        switch (stmt) {
            .peers => {
                const peers = self.sync_mgr.getPeerList(self.allocator) catch {
                    sendError(stream, 500, "Failed to get peer list");
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

                writer.writeAll("{\"columns\":[\"peer_node_id\",\"desktop_name\",\"address\",\"status\"],\"rows\":[") catch return;
                for (peers, 0..) |p, i| {
                    if (i > 0) writer.writeAll(",") catch return;
                    writer.writeAll("[\"") catch return;
                    writer.writeAll(&p.node_id_hex) catch return;
                    writer.writeAll("\",") catch return;
                    writeJsonString(writer, p.desktop_name) catch return;
                    writer.writeAll(",") catch return;
                    writeJsonString(writer, p.address) catch return;
                    writer.writeAll(",") catch return;
                    writeJsonString(writer, p.status) catch return;
                    writer.writeAll("]") catch return;
                }
                writer.writeAll("],\"message\":\"Active desktop peer list\"}") catch return;

                const swriter = stream.writer();
                swriter.print(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
                    .{ json_buf.items.len, json_buf.items },
                ) catch {};
                return;
            },
            .sync_connect => |sc| {
                var it = std.mem.splitScalar(u8, sc.address, ':');
                const host = it.next() orelse "127.0.0.1";
                const port_str = it.next() orelse "7433";
                const port = std.fmt.parseInt(u16, port_str, 10) catch 7433;

                self.sync_mgr.connectToPeer(host, port) catch |err| {
                    var err_buf: [128]u8 = undefined;
                    const err_msg = std.fmt.bufPrint(&err_buf, "Sync connect error: {s}", .{@errorName(err)}) catch "Sync connect error";
                    sendError(stream, 500, err_msg);
                    return;
                };

                var json_buf = std.ArrayList(u8).init(self.allocator);
                defer json_buf.deinit();
                const writer = json_buf.writer();
                var msg_buf: [128]u8 = undefined;
                const msg = std.fmt.bufPrint(&msg_buf, "Successfully connected and synchronized with desktop peer at {s}:{d}!", .{ host, port }) catch "Connected to peer.";
                writer.writeAll("{\"columns\":[],\"rows\":[],\"message\":") catch return;
                writeJsonString(writer, msg) catch return;
                writer.writeAll("}") catch return;

                const swriter = stream.writer();
                swriter.print(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
                    .{ json_buf.items.len, json_buf.items },
                ) catch {};
                return;
            },
            else => {},
        }

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
        var body_buf: [1024]u8 = undefined;
        const body = std.fmt.bufPrint(&body_buf, "{{\"error\":\"{s}\"}}", .{msg}) catch "{\"error\":\"Unknown error\"}";
        const writer = stream.writer();
        writer.print(
            "HTTP/1.1 {d} Error\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}",
            .{ code, body.len, body },
        ) catch {};
    }
};
