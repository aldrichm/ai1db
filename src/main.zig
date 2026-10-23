const std = @import("std");
const types = @import("types.zig");
const Engine = @import("storage/engine.zig").Engine;
const SyncManager = @import("sync/node.zig").SyncManager;
const DiscoveryBeacon = @import("sync/discovery.zig").DiscoveryBeacon;
const WebServer = @import("web/server.zig").WebServer;
const Parser = @import("sql/parser.zig").Parser;
const Executor = @import("sql/executor.zig").Executor;

const BANNER =
    \\
    \\   ███████╗████████╗██████╗  █████╗ ████████╗ █████╗ ██████╗ ██████╗ 
    \\   ██╔════╝╚══██╔══╝██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗
    \\   ███████╗   ██║   ██████╔╝███████║   ██║   ███████║██║  ██║██████╔╝
    \\   ╚════██║   ██║   ██╔══██╗██╔══██║   ██║   ██╔══██║██║  ██║██╔══██╗
    \\   ███████║   ██║   ██║  ██║██║  ██║   ██║   ██║  ██║██████╔╝██████╔╝
    \\   ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚═════╝ 
    \\           Distributed Relational Database for Connected Desktops
    \\
;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Default configuration
    var desktop_name: []const u8 = "Desktop-Node";
    var data_dir: []const u8 = "./stratadb_data";
    var sync_port: u16 = 7433;
    var web_port: u16 = 8080;
    var discovery_port: u16 = 7432;
    var enable_discovery = true;
    var connect_peer: ?[]const u8 = null;
    var exec_query: ?[]const u8 = null;
    var is_daemon = false;

    // Parse command line arguments
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // skip exe name

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--name") or std.mem.eql(u8, arg, "-n")) {
            desktop_name = args.next() orelse desktop_name;
        } else if (std.mem.eql(u8, arg, "--data") or std.mem.eql(u8, arg, "-d")) {
            data_dir = args.next() orelse data_dir;
        } else if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            const p_str = args.next() orelse "7433";
            sync_port = std.fmt.parseInt(u16, p_str, 10) catch 7433;
        } else if (std.mem.eql(u8, arg, "--web-port") or std.mem.eql(u8, arg, "-w")) {
            const w_str = args.next() orelse "8080";
            web_port = std.fmt.parseInt(u16, w_str, 10) catch 8080;
        } else if (std.mem.eql(u8, arg, "--connect") or std.mem.eql(u8, arg, "-c")) {
            connect_peer = args.next();
        } else if (std.mem.eql(u8, arg, "--exec") or std.mem.eql(u8, arg, "-e")) {
            exec_query = args.next();
        } else if (std.mem.eql(u8, arg, "--discovery-port")) {
            const dp_str = args.next() orelse "7432";
            discovery_port = std.fmt.parseInt(u16, dp_str, 10) catch 7432;
        } else if (std.mem.eql(u8, arg, "--no-discovery")) {
            enable_discovery = false;
        } else if (std.mem.eql(u8, arg, "--daemon") or std.mem.eql(u8, arg, "--server")) {
            is_daemon = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        }
    }

    // Initialize Database Storage Engine
    const engine = try Engine.init(allocator, data_dir, desktop_name);
    defer engine.deinit();

    // Initialize Sync Manager (TCP Server & Peer Coordinator)
    const sync_mgr = try SyncManager.init(allocator, engine, sync_port);
    defer sync_mgr.deinit();
    try sync_mgr.start();

    // Initialize LAN UDP Discovery Beacon
    var beacon: ?*DiscoveryBeacon = null;
    if (enable_discovery) {
        beacon = try DiscoveryBeacon.init(allocator, sync_mgr, discovery_port);
        try beacon.?.start();
    }
    defer if (beacon) |b| b.deinit();

    // Initialize Desktop Web Dashboard
    const web_server = try WebServer.init(allocator, engine, sync_mgr, web_port);
    defer web_server.deinit();
    try web_server.start();

    // Auto-connect to specified peer if provided
    if (connect_peer) |peer_addr| {
        var it = std.mem.splitScalar(u8, peer_addr, ':');
        const host = it.next() orelse "127.0.0.1";
        const port_str = it.next() orelse "7433";
        const port = std.fmt.parseInt(u16, port_str, 10) catch 7433;
        sync_mgr.connectToPeer(host, port) catch |err| {
            std.debug.print("Failed to connect to peer {s}:{d}: {any}\n", .{ host, port, err });
        };
    }

    const stdout = std.io.getStdOut().writer();

    // If --exec passed, execute single query and terminate
    if (exec_query) |query| {
        try executeSingle(allocator, engine, sync_mgr, query, stdout);
        return;
    }

    // Interactive REPL Shell
    try stdout.print("{s}", .{BANNER});
    var node_hex: [32]u8 = undefined;
    _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&engine.node_id)}) catch "";

    try stdout.print("Desktop Node:     \x1b[32m{s}\x1b[0m (ID: {s})\n", .{ desktop_name, node_hex[0..8] });
    try stdout.print("Data Directory:   {s}\n", .{data_dir});
    try stdout.print("P2P Sync Server:  \x1b[36m0.0.0.0:{d}\x1b[0m (TCP Live Auto-Sync)\n", .{sync_port});
    try stdout.print("LAN Discovery:    \x1b[36m255.255.255.255:{d}\x1b[0m (UDP Auto-Detect: {s})\n", .{ discovery_port, if (enable_discovery) "ENABLED" else "DISABLED" });
    try stdout.print("Desktop Web UI:   \x1b[33mhttp://localhost:{d}\x1b[0m\n\n", .{web_port});

    if (is_daemon) {
        try stdout.print("\x1b[32mStrataDB is running in background daemon mode.\x1b[0m\n", .{});
        try stdout.print("Web Dashboard live at: \x1b[1;34mhttp://localhost:{d}\x1b[0m\n", .{web_port});
        while (true) {
            std.time.sleep(1_000_000_000);
        }
    }

    try stdout.print("Type \x1b[1mHELP;\x1b[0m for commands or \x1b[1mEXIT;\x1b[0m to quit.\n\n", .{});

    const stdin = std.io.getStdIn().reader();
    var line_buf: [4096]u8 = undefined;

    while (true) {
        try stdout.print("stratadb> ", .{});

        const line_opt = stdin.readUntilDelimiterOrEof(&line_buf, '\n') catch null;
        if (line_opt == null) {
            // If stdin is closed / non-interactive (e.g. background process), stay alive
            std.time.sleep(1_000_000_000);
            continue;
        }

        const line = std.mem.trim(u8, line_opt.?, " \r\t");
        if (line.len == 0) continue;

        if (std.ascii.eqlIgnoreCase(line, "exit") or std.ascii.eqlIgnoreCase(line, "exit;") or
            std.ascii.eqlIgnoreCase(line, "quit") or std.ascii.eqlIgnoreCase(line, "quit;"))
        {
            try stdout.print("Exiting StrataDB. Synchronized state checkpointed.\n", .{});
            break;
        }

        executeSingle(allocator, engine, sync_mgr, line, stdout) catch |err| {
            try stdout.print("\x1b[31mError: {any}\x1b[0m\n", .{err});
        };
    }
}

fn executeSingle(
    allocator: std.mem.Allocator,
    engine: *Engine,
    sync_mgr: *SyncManager,
    query: []const u8,
    writer: anytype,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var parser = Parser.init(arena_alloc, query) catch |err| {
        try writer.print("\x1b[31mSyntax/Lexer error: {any}\x1b[0m\n", .{err});
        return;
    };
    defer parser.deinit();

    const stmt = parser.parse() catch |err| {
        try writer.print("\x1b[31mParse error: {any}\x1b[0m\n", .{err});
        return;
    };

    switch (stmt) {
        .peers => {
            const peers = try sync_mgr.getPeerList(allocator);
            defer {
                for (peers) |p| {
                    allocator.free(p.desktop_name);
                    allocator.free(p.address);
                }
                allocator.free(peers);
            }

            if (peers.len == 0) {
                try writer.print("No connected desktop peers currently active.\n", .{});
                return;
            }

            var col_names = try allocator.alloc([]const u8, 4);
            col_names[0] = try allocator.dupe(u8, "peer_node_id");
            col_names[1] = try allocator.dupe(u8, "desktop_name");
            col_names[2] = try allocator.dupe(u8, "address");
            col_names[3] = try allocator.dupe(u8, "status");

            var rows = try allocator.alloc([]types.Value, peers.len);
            for (peers, 0..) |p, i| {
                rows[i] = try allocator.alloc(types.Value, 4);
                rows[i][0] = types.Value{ .text = try allocator.dupe(u8, &p.node_id_hex) };
                rows[i][1] = types.Value{ .text = try allocator.dupe(u8, p.desktop_name) };
                rows[i][2] = types.Value{ .text = try allocator.dupe(u8, p.address) };
                rows[i][3] = types.Value{ .text = try allocator.dupe(u8, p.status) };
            }

            var res = @import("sql/executor.zig").QueryResult{
                .allocator = allocator,
                .columns = col_names,
                .rows = rows,
            };
            defer res.deinit();
            try res.renderAsciiTable(writer);
            return;
        },
        .sync_connect => |sc| {
            var it = std.mem.splitScalar(u8, sc.address, ':');
            const host = it.next() orelse "127.0.0.1";
            const port_str = it.next() orelse "7433";
            const port = std.fmt.parseInt(u16, port_str, 10) catch 7433;
            try writer.print("Connecting to peer at {s}:{d}...\n", .{ host, port });
            try sync_mgr.connectToPeer(host, port);
            try writer.print("\x1b[32mSuccessfully connected and synchronized with desktop peer!\x1b[0m\n", .{});
            return;
        },
        else => {},
    }

    var executor = Executor.init(allocator, engine);
    var res = try executor.execute(stmt);
    defer res.deinit();

    try res.renderAsciiTable(writer);
}

fn printUsage() void {
    const usage =
        \\StrataDB - Distributed Relational Database for Connected Desktops
        \\
        \\Usage:
        \\  stratadb [options]
        \\
        \\Options:
        \\  -n, --name <name>          Custom desktop name (default: "Desktop-Node")
        \\  -d, --data <dir>           Database directory (default: "./stratadb_data")
        \\  -p, --port <port>          P2P TCP sync port (default: 7433)
        \\  -w, --web-port <port>      Desktop Web UI port (default: 8080)
        \\  -c, --connect <host:port>  Connect to remote desktop on startup
        \\  -e, --exec <sql>           Execute a single SQL command and exit
        \\      --server, --daemon     Run in server/daemon mode without interactive REPL
        \\      --no-discovery         Disable automatic UDP LAN broadcast discovery
        \\  -h, --help                 Show this help message
        \\
    ;
    std.io.getStdOut().writer().print("{s}", .{usage}) catch {};
}
