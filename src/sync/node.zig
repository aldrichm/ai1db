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

pub const PeerConnection = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    node_id: [16]u8,
    desktop_name: []const u8,
    address_str: []const u8,
    last_hlc: HLC,
    is_alive: bool = true,
    mutex: std.Thread.Mutex,

    pub fn sendFrame(self: *PeerConnection, msg_type: MessageType, payload: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try Frame.write(self.stream.writer(), msg_type, payload);
    }

    pub fn deinit(self: *PeerConnection) void {
        self.is_alive = false;
        self.stream.close();
        self.allocator.free(self.desktop_name);
        self.allocator.free(self.address_str);
        self.allocator.destroy(self);
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

    pub fn init(allocator: std.mem.Allocator, engine: *Engine, listen_port: u16) !*SyncManager {
        const mgr = try allocator.create(SyncManager);
        mgr.* = SyncManager{
            .allocator = allocator,
            .engine = engine,
            .listen_port = listen_port,
            .active_peers = std.ArrayList(*PeerConnection).init(allocator),
            .peers_mutex = std.Thread.Mutex{},
        };

        // Wire up engine mutation listener to broadcast in real time
        engine.setMutationListener(mgr, onLocalMutation);

        return mgr;
    }

    pub fn deinit(self: *SyncManager) void {
        self.server_running = false;

        self.peers_mutex.lock();
        for (self.active_peers.items) |peer| {
            peer.deinit();
        }
        self.active_peers.deinit();
        self.peers_mutex.unlock();

        self.allocator.destroy(self);
    }

    fn onLocalMutation(ctx: *anyopaque, record: ChangeRecord) void {
        const self: *SyncManager = @ptrCast(@alignCast(ctx));
        self.broadcastLiveMutation(record);
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
            _ = std.Thread.spawn(.{}, handleIncomingPeer, .{ self, conn.stream, conn.address }) catch {
                conn.stream.close();
            };
        }
    }

    fn handleIncomingPeer(self: *SyncManager, stream: std.net.Stream, addr: std.net.Address) void {
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
        ack_msg.serialize(ack_buf.writer()) catch return;
        Frame.write(stream.writer(), .hello_ack, ack_buf.items) catch return;

        // Register peer
        const peer = self.allocator.create(PeerConnection) catch return;
        peer.* = PeerConnection{
            .allocator = self.allocator,
            .stream = stream,
            .node_id = hello_msg.node_id,
            .desktop_name = self.allocator.dupe(u8, hello_msg.desktop_name) catch return,
            .address_str = self.allocator.dupe(u8, addr_str) catch return,
            .last_hlc = hello_msg.current_hlc,
            .mutex = std.Thread.Mutex{},
        };

        self.addPeer(peer);
        defer self.removePeer(peer);

        // Initial Delta Sync: send our changes since their HLC
        self.sendDeltaSync(peer, hello_msg.current_hlc) catch {};

        // Run message processing loop
        self.peerMessageLoop(peer);
    }

    pub fn connectToPeer(self: *SyncManager, host: []const u8, port: u16) !void {
        const address = try std.net.Address.parseIp4(host, port);
        const stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

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

        const addr_str = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ host, port });
        errdefer self.allocator.free(addr_str);

        const peer = try self.allocator.create(PeerConnection);
        peer.* = PeerConnection{
            .allocator = self.allocator,
            .stream = stream,
            .node_id = ack_msg.node_id,
            .desktop_name = try self.allocator.dupe(u8, ack_msg.desktop_name),
            .address_str = addr_str,
            .last_hlc = ack_msg.current_hlc,
            .mutex = std.Thread.Mutex{},
        };

        self.addPeer(peer);

        // Spawn background worker for this connected peer
        _ = try std.Thread.spawn(.{}, peerWorkerThread, .{ self, peer });
    }

    fn peerWorkerThread(self: *SyncManager, peer: *PeerConnection) void {
        defer self.removePeer(peer);
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

        while (peer.is_alive and self.server_running) {
            const frame = Frame.read(self.allocator, reader) catch break;
            defer self.allocator.free(frame.payload);

            switch (frame.msg_type) {
                .sync_request => {
                    var fbs = std.io.fixedBufferStream(frame.payload);
                    const req = SyncRequestMessage.deserialize(fbs.reader()) catch continue;
                    self.sendDeltaSync(peer, req.since_hlc) catch {};
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
                .ping => {
                    peer.sendFrame(.pong, &[_]u8{}) catch break;
                },
                else => {},
            }
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
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        record.serialize(buf.writer()) catch return;

        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();

        for (self.active_peers.items) |peer| {
            peer.sendFrame(.live_mutation, buf.items) catch {
                peer.is_alive = false;
            };
        }
    }

    fn addPeer(self: *SyncManager, peer: *PeerConnection) void {
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();
        self.active_peers.append(peer) catch {};
    }

    fn removePeer(self: *SyncManager, peer: *PeerConnection) void {
        self.peers_mutex.lock();
        defer self.peers_mutex.unlock();

        for (self.active_peers.items, 0..) |p, i| {
            if (p == peer) {
                _ = self.active_peers.swapRemove(i);
                peer.deinit();
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
                .status = if (peer.is_alive) "CONNECTED" else "DISCONNECTED",
                .last_sync_hlc = peer.last_hlc,
            };
        }
        return list;
    }
};
