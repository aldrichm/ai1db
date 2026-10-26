const std = @import("std");
const types = @import("../types.zig");
const Engine = @import("../storage/engine.zig").Engine;
const HLC = @import("hlc.zig").HLC;
const cdc = @import("cdc.zig");
const ChangeRecord = cdc.ChangeRecord;
const protocol = @import("protocol.zig");
const Frame = protocol.Frame;
const MessageType = protocol.MessageType;
const HelloMessage = protocol.HelloMessage;
const SyncRequestMessage = protocol.SyncRequestMessage;

pub const PeerInfo = struct {
    node_id_hex: [32]u8,
    desktop_name: []const u8,
    address: []const u8,
    status: []const u8,
    last_sync_hlc: HLC,
};

fn configureSocket(stream: std.net.Stream) void {
    const handle = stream.handle;
    if (@hasDecl(std.posix.SO, "NOSIGPIPE")) {
        const val: c_int = 1;
        std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.NOSIGPIPE, std.mem.asBytes(&val)) catch {};
    }
    {
        const val: c_int = 1;
        std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&val)) catch {};
    }
    {
        const val: c_int = 1;
        // IPPROTO_TCP = 6, TCP_NODELAY = 1
        std.posix.setsockopt(handle, 6, 1, std.mem.asBytes(&val)) catch {};
    }
    {
        const timeout = std.posix.timeval{ .tv_sec = 60, .tv_usec = 0 };
        std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&timeout)) catch {};
    }
    {
        const timeout = std.posix.timeval{ .tv_sec = 60, .tv_usec = 0 };
        std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout)) catch {};
    }
}

pub const PeerConnection = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    node_id: [16]u8,
    desktop_name: []const u8,
    address_str: []const u8,
    last_hlc: HLC,
    is_alive: bool = true,
    is_closed: bool = false,
    ref_count: std.atomic.Value(u32),
    mutex: std.Thread.Mutex,

    pub fn init(
        allocator: std.mem.Allocator,
        stream: std.net.Stream,
        node_id: [16]u8,
        desktop_name: []const u8,
        address_str: []const u8,
        last_hlc: HLC,
    ) !*PeerConnection {
        const peer = try allocator.create(PeerConnection);
        peer.* = PeerConnection{
            .allocator = allocator,
            .stream = stream,
            .node_id = node_id,
            .desktop_name = try allocator.dupe(u8, desktop_name),
            .address_str = try allocator.dupe(u8, address_str),
            .last_hlc = last_hlc,
            .is_alive = true,
            .is_closed = false,
            .ref_count = std.atomic.Value(u32).init(1),
            .mutex = std.Thread.Mutex{},
        };
        return peer;
    }

    pub fn ref(self: *PeerConnection) *PeerConnection {
        _ = self.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn unref(self: *PeerConnection) void {
        if (self.ref_count.fetchSub(1, .release) == 1) {
            self.ref_count.fence(.acquire);
            self.close();
            self.allocator.free(self.desktop_name);
            self.allocator.free(self.address_str);
            self.allocator.destroy(self);
        }
    }

    pub fn close(self: *PeerConnection) void {
        self.mutex.lock();
        if (!self.is_closed) {
            self.is_closed = true;
            self.is_alive = false;
            _ = std.posix.shutdown(self.stream.handle, .both) catch {};
            self.stream.close();
        }
        self.mutex.unlock();
    }

    pub fn sendFrame(self: *PeerConnection, msg_type: MessageType, payload: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.is_closed or !self.is_alive) return error.ConnectionClosed;
        try Frame.write(self.stream.writer(), msg_type, payload);
    }
};

pub const SyncManager = struct {
    allocator: std.mem.Allocator,
    engine: *Engine,
    listen_port: u16,
    server_running: bool = false,
    active_peers: std.ArrayList(*PeerConnection),
    peers_mutex: std.Thread.Mutex,
    server_thread: ?std.Thread = null,
    active_threads: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn init(allocator: std.mem.Allocator, engine: *Engine, listen_port: u16) !*SyncManager {
        const mgr = try allocator.create(SyncManager);
        mgr.* = SyncManager{
            .allocator = allocator,
            .engine = engine,
            .listen_port = listen_port,
            .active_peers = std.ArrayList(*PeerConnection).init(allocator),
            .peers_mutex = std.Thread.Mutex{},
            .active_threads = std.atomic.Value(usize).init(0),
        };

        // Wire up engine mutation and DDL listeners to broadcast in real time
        engine.setMutationListener(mgr, onLocalMutation);
        engine.setDdlListener(mgr, onLocalDdl);

        return mgr;
    }

    pub fn deinit(self: *SyncManager) void {
        self.server_running = false;

        // Wake up listening socket to terminate accept loop
        if (std.net.tcpConnectToHost(self.allocator, "127.0.0.1", self.listen_port)) |stream| {
            stream.close();
        } else |_| {}

        if (self.server_thread) |t| {
            t.join();
            self.server_thread = null;
        }

        // Close all peer connections to unblock pending reads/writes
        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            peer.close();
        }
        self.peers_mutex.unlock();

        // Wait for all worker threads to cleanly exit
        while (self.active_threads.load(.acquire) > 0) {
            std.time.sleep(5 * std.time.ns_per_ms);
        }

        // Release references held by active_peers
        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            peer.unref();
        }
        self.active_peers.deinit();
        self.peers_mutex.unlock();

        self.allocator.destroy(self);
    }

    fn onLocalMutation(ctx: *anyopaque, record: ChangeRecord) void {
        const self: *SyncManager = @ptrCast(@alignCast(ctx));
        self.broadcastLiveMutation(record);
    }

    fn onLocalDdl(ctx: *anyopaque, table_name: []const u8, is_drop: bool) void {
        const self: *SyncManager = @ptrCast(@alignCast(ctx));
        if (is_drop) {
            self.broadcastDdlDrop(table_name);
        } else {
            self.broadcastTableSync(table_name);
        }
    }

    pub fn hasActivePeers(self: *SyncManager) bool {
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();
        for (self.active_peers.items) |peer| {
            if (peer.is_alive and !peer.is_closed) return true;
        }
        return false;
    }

    pub fn broadcastTableSync(self: *SyncManager, table_name: []const u8) void {
        if (!self.hasActivePeers()) return;
        _ = self.engine.getTableRowCount(table_name) orelse return;
        const chunk_size: usize = 20000;
        var offset: usize = 0;

        while (true) {
            const snap_opt = self.engine.createTableSnapshotChunked(table_name, self.allocator, offset, chunk_size) catch break;
            if (snap_opt == null) break;
            var snap = snap_opt.?;
            defer snap.deinit(self.allocator);

            var buf = std.ArrayList(u8).init(self.allocator);
            defer buf.deinit();
            const writer = buf.writer();

            writer.writeInt(u32, 1, .little) catch break;
            snap.serialize(writer) catch break;

            // Snapshot peers under lock, send outside lock
            var peers_to_send = std.ArrayList(*PeerConnection).init(self.allocator);
            defer {
                for (peers_to_send.items) |peer| peer.unref();
                peers_to_send.deinit();
            }

            self.peers_mutex.lock();
            for (self.active_peers.items) |peer| {
                if (peer.is_alive and !peer.is_closed) {
                    peers_to_send.append(peer.ref()) catch continue;
                }
            }
            self.peers_mutex.unlock();

            for (peers_to_send.items) |peer| {
                peer.sendFrame(.table_sync, buf.items) catch {
                    peer.close();
                };
            }

            offset += snap.rows.len;
            if (snap.rows.len < chunk_size) break;
        }
    }

    pub fn broadcastDdlDrop(self: *SyncManager, table_name: []const u8) void {
        if (!self.hasActivePeers()) return;
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        const writer = buf.writer();

        writer.writeInt(u32, @intCast(table_name.len), .little) catch return;
        writer.writeAll(table_name) catch return;

        var peers_to_send = std.ArrayList(*PeerConnection).init(self.allocator);
        defer {
            for (peers_to_send.items) |peer| peer.unref();
            peers_to_send.deinit();
        }

        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            if (peer.is_alive and !peer.is_closed) {
                peers_to_send.append(peer.ref()) catch continue;
            }
        }
        self.peers_mutex.unlock();

        for (peers_to_send.items) |peer| {
            peer.sendFrame(.ddl_drop, buf.items) catch {
                peer.close();
            };
        }
    }

    pub fn start(self: *SyncManager) !void {
        self.server_running = true;
        self.server_thread = try std.Thread.spawn(.{}, serverWorker, .{self});
    }

    fn serverWorker(self: *SyncManager) void {
        const address = std.net.Address.parseIp4("0.0.0.0", self.listen_port) catch return;
        var server = address.listen(.{ .reuse_address = true }) catch return;
        defer server.deinit();

        while (self.server_running) {
            const conn = server.accept() catch continue;
            if (!self.server_running) {
                conn.stream.close();
                break;
            }
            _ = std.Thread.spawn(.{}, handleIncomingPeer, .{ self, conn.stream, conn.address }) catch {
                conn.stream.close();
            };
        }
    }

    fn handleIncomingPeer(self: *SyncManager, stream: std.net.Stream, addr: std.net.Address) void {
        _ = self.active_threads.fetchAdd(1, .release);
        defer _ = self.active_threads.fetchSub(1, .release);

        configureSocket(stream);

        var addr_buf: [64]u8 = undefined;
        const addr_str = std.fmt.bufPrint(&addr_buf, "{any}", .{addr}) catch "unknown";

        // Read Hello frame
        const frame = Frame.read(self.allocator, stream.reader()) catch {
            stream.close();
            return;
        };
        defer self.allocator.free(frame.payload);

        if (frame.msg_type != .hello) {
            stream.close();
            return;
        }

        var fbs = std.io.fixedBufferStream(frame.payload);
        const hello_msg = HelloMessage.deserialize(self.allocator, fbs.reader()) catch {
            stream.close();
            return;
        };
        defer self.allocator.free(hello_msg.desktop_name);

        // Don't connect to self
        if (std.mem.eql(u8, &hello_msg.node_id, &self.engine.node_id)) {
            stream.close();
            return;
        }

        // Send Hello Ack
        var ack_buf = std.ArrayList(u8).init(self.allocator);
        defer ack_buf.deinit();
        const ack_msg = HelloMessage{
            .node_id = self.engine.node_id,
            .desktop_name = self.engine.desktop_name,
            .current_hlc = self.engine.hlc,
            .listen_port = self.listen_port,
        };
        ack_msg.serialize(ack_buf.writer()) catch {
            stream.close();
            return;
        };
        Frame.write(stream.writer(), .hello_ack, ack_buf.items) catch {
            stream.close();
            return;
        };

        // Register peer
        const peer = PeerConnection.init(
            self.allocator,
            stream,
            hello_msg.node_id,
            hello_msg.desktop_name,
            addr_str,
            hello_msg.current_hlc,
        ) catch {
            stream.close();
            return;
        };
        defer peer.unref();

        if (!self.addPeer(peer)) {
            peer.close();
            return;
        }
        defer self.removePeer(peer);

        // Send all schemas & table snapshots to the new peer
        self.sendAllTablesSync(peer) catch {};

        // Initial Delta Sync: send our changes since their HLC
        self.sendDeltaSync(peer, hello_msg.current_hlc) catch {};

        // Run message processing loop
        self.peerMessageLoop(peer);
    }

    pub fn connectToPeer(self: *SyncManager, host: []const u8, port: u16) !void {
        const address = try std.net.Address.parseIp4(host, port);
        const stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();
        configureSocket(stream);

        // Send Hello
        var hello_buf = std.ArrayList(u8).init(self.allocator);
        defer hello_buf.deinit();
        const hello_msg = HelloMessage{
            .node_id = self.engine.node_id,
            .desktop_name = self.engine.desktop_name,
            .current_hlc = self.engine.hlc,
            .listen_port = self.listen_port,
        };
        try hello_msg.serialize(hello_buf.writer());
        try Frame.write(stream.writer(), .hello, hello_buf.items);

        // Read Hello Ack
        const frame = try Frame.read(self.allocator, stream.reader());
        defer self.allocator.free(frame.payload);

        if (frame.msg_type != .hello_ack) {
            return error.HandshakeFailed;
        }

        var fbs = std.io.fixedBufferStream(frame.payload);
        const ack_msg = try HelloMessage.deserialize(self.allocator, fbs.reader());
        defer self.allocator.free(ack_msg.desktop_name);

        // Don't connect to self
        if (std.mem.eql(u8, &ack_msg.node_id, &self.engine.node_id)) {
            return error.CannotConnectToSelf;
        }

        const addr_str = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ host, port });
        defer self.allocator.free(addr_str);

        const peer = try PeerConnection.init(
            self.allocator,
            stream,
            ack_msg.node_id,
            ack_msg.desktop_name,
            addr_str,
            ack_msg.current_hlc,
        );
        errdefer peer.unref();

        if (!self.addPeer(peer)) {
            // Already connected to this peer
            peer.close();
            peer.unref();
            return;
        }

        // Spawn background worker for this connected peer
        _ = try std.Thread.spawn(.{}, peerWorkerThread, .{ self, peer });
    }

    fn peerWorkerThread(self: *SyncManager, peer: *PeerConnection) void {
        _ = self.active_threads.fetchAdd(1, .release);
        defer {
            self.removePeer(peer);
            peer.unref();
            _ = self.active_threads.fetchSub(1, .release);
        }

        // Send our existing schemas & data to remote peer
        self.sendAllTablesSync(peer) catch {};

        // Request delta sync from remote peer
        var req_buf = std.ArrayList(u8).init(self.allocator);
        defer req_buf.deinit();
        const req_msg = SyncRequestMessage{ .since_hlc = self.engine.hlc };
        req_msg.serialize(req_buf.writer()) catch return;
        peer.sendFrame(.sync_request, req_buf.items) catch return;

        self.peerMessageLoop(peer);
    }

    fn peerMessageLoop(self: *SyncManager, peer: *PeerConnection) void {
        const reader = peer.stream.reader();

        while (peer.is_alive and !peer.is_closed and self.server_running) {
            const frame = Frame.read(self.allocator, reader) catch |err| {
                if (err == error.WouldBlock) {
                    // Timeout heartbeat check
                    peer.sendFrame(.ping, &[_]u8{}) catch break;
                    continue;
                }
                break;
            };
            defer self.allocator.free(frame.payload);

            switch (frame.msg_type) {
                .ping => {
                    peer.sendFrame(.pong, &[_]u8{}) catch break;
                },
                .pong => {},
                .sync_request => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    const req = SyncRequestMessage.deserialize(fbs.reader()) catch continue;
                    self.sendAllTablesSync(peer) catch {};
                    self.sendDeltaSync(peer, req.since_hlc) catch {};
                },
                .table_sync => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    self.applyTableSyncPayload(fbs.reader()) catch {};
                },
                .ddl_drop => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    const r = fbs.reader();
                    const name_len = r.readInt(u32, .little) catch continue;
                    const name = self.allocator.alloc(u8, name_len) catch continue;
                    defer self.allocator.free(name);
                    r.readNoEof(name) catch continue;
                    _ = self.engine.dropTableInternal(name) catch {};
                },
                .sync_response => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    const count = fbs.reader().readInt(u32, .little) catch continue;
                    var i: usize = 0;
                    while (i < count) : (i += 1) {
                        const rec = ChangeRecord.deserialize(self.allocator, fbs.reader()) catch break;
                        defer rec.deinit(self.allocator);
                        _ = self.engine.applyRemoteChange(rec) catch {};
                    }
                },
                .live_mutation => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    const rec = ChangeRecord.deserialize(self.allocator, fbs.reader()) catch continue;
                    defer rec.deinit(self.allocator);
                    _ = self.engine.applyRemoteChange(rec) catch {};
                },
                else => {},
            }
        }
    }

    pub fn sendAllTablesSync(self: *SyncManager, peer: *PeerConnection) !void {
        const table_names = try self.engine.listTables(self.allocator);
        defer {
            for (table_names) |name| self.allocator.free(name);
            self.allocator.free(table_names);
        }

        for (table_names) |table_name| {
            _ = self.engine.getTableRowCount(table_name) orelse continue;
            const chunk_size: usize = 20000;
            var offset: usize = 0;

            while (true) {
                const snap_opt = try self.engine.createTableSnapshotChunked(table_name, self.allocator, offset, chunk_size);
                if (snap_opt == null) break;
                var snap = snap_opt.?;
                defer snap.deinit(self.allocator);

                var buf = std.ArrayList(u8).init(self.allocator);
                defer buf.deinit();
                const writer = buf.writer();

                try writer.writeInt(u32, 1, .little);
                try snap.serialize(writer);

                try peer.sendFrame(.table_sync, buf.items);

                offset += snap.rows.len;
                if (snap.rows.len < chunk_size) break;
            }
        }
    }

    pub fn broadcastAllTablesSync(self: *SyncManager) !void {
        var peers_to_send = std.ArrayList(*PeerConnection).init(self.allocator);
        defer {
            for (peers_to_send.items) |peer| peer.unref();
            peers_to_send.deinit();
        }

        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            if (peer.is_alive and !peer.is_closed) {
                try peers_to_send.append(peer.ref());
            }
        }
        self.peers_mutex.unlock();

        for (peers_to_send.items) |peer| {
            self.sendAllTablesSync(peer) catch {
                peer.close();
            };
        }
    }

    pub fn applyTableSyncPayload(self: *SyncManager, reader: anytype) !void {
        const count = try reader.readInt(u32, .little);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            var snap = try protocol.TableSnapshot.deserialize(self.allocator, reader);
            defer snap.deinit(self.allocator);
            try self.engine.applyTableSnapshot(snap);
        }
    }

    fn sendDeltaSync(self: *SyncManager, peer: *PeerConnection, since_hlc: ?HLC) !void {
        const changes = try self.engine.cdc_log.getChangesSince(since_hlc, self.allocator);
        defer {
            for (changes) |c| c.deinit(self.allocator);
            self.allocator.free(changes);
        }

        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        const writer = buf.writer();

        try writer.writeInt(u32, @intCast(changes.len), .little);
        for (changes) |change| {
            try change.serialize(writer);
        }

        try peer.sendFrame(.sync_response, buf.items);
    }

    pub fn broadcastLiveMutation(self: *SyncManager, record: ChangeRecord) void {
        if (!self.hasActivePeers()) return;
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        record.serialize(buf.writer()) catch return;

        var peers_to_send = std.ArrayList(*PeerConnection).init(self.allocator);
        defer {
            for (peers_to_send.items) |peer| peer.unref();
            peers_to_send.deinit();
        }

        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            if (peer.is_alive and !peer.is_closed) {
                peers_to_send.append(peer.ref()) catch continue;
            }
        }
        self.peers_mutex.unlock();

        for (peers_to_send.items) |peer| {
            peer.sendFrame(.live_mutation, buf.items) catch {
                peer.close();
            };
        }
    }

    fn addPeer(self: *SyncManager, peer: *PeerConnection) bool {
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();

        var i: usize = 0;
        while (i < self.active_peers.items.len) {
            const existing = self.active_peers.items[i];
            if (std.mem.eql(u8, &existing.node_id, &peer.node_id)) {
                if (existing.is_alive and !existing.is_closed) {
                    return false;
                } else {
                    _ = self.active_peers.swapRemove(i);
                    existing.unref();
                    continue;
                }
            }
            i += 1;
        }

        self.active_peers.append(peer.ref()) catch return false;
        return true;
    }

    fn removePeer(self: *SyncManager, peer: *PeerConnection) void {
        peer.close();
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();

        for (self.active_peers.items, 0..) |p, i| {
            if (p == peer) {
                _ = self.active_peers.swapRemove(i);
                peer.unref();
                break;
            }
        }
    }

    pub fn getPeerList(self: *SyncManager, allocator: std.mem.Allocator) ![]PeerInfo {
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();

        var list = try allocator.alloc(PeerInfo, self.active_peers.items.len);
        for (self.active_peers.items, 0..) |peer, i| {
            var hex: [32]u8 = undefined;
            _ = std.fmt.bufPrint(&hex, "{s}", .{std.fmt.fmtSliceHexLower(&peer.node_id)}) catch "";
            list[i] = PeerInfo{
                .node_id_hex = hex,
                .desktop_name = try allocator.dupe(u8, peer.desktop_name),
                .address = try allocator.dupe(u8, peer.address_str),
                .status = if (peer.is_alive and !peer.is_closed) "CONNECTED" else "DISCONNECTED",
                .last_sync_hlc = peer.last_hlc,
            };
        }
        return list;
    }
};
