const std = @import("std");
const SyncManager = @import("node.zig").SyncManager;

pub const DiscoveryBeacon = struct {
    allocator: std.mem.Allocator,
    sync_mgr: *SyncManager,
    broadcast_port: u16,
    running: bool = false,
    beacon_thread: ?std.Thread = null,
    listener_thread: ?std.Thread = null,

    pub fn init(allocator: std.mem.Allocator, sync_mgr: *SyncManager, broadcast_port: u16) !*DiscoveryBeacon {
        const beacon = try allocator.create(DiscoveryBeacon);
        beacon.* = DiscoveryBeacon{
            .allocator = allocator,
            .sync_mgr = sync_mgr,
            .broadcast_port = broadcast_port,
            .running = false,
        };
        return beacon;
    }

    pub fn deinit(self: *DiscoveryBeacon) void {
        self.running = false;
        self.allocator.destroy(self);
    }

    pub fn start(self: *DiscoveryBeacon) !void {
        self.running = true;
        self.listener_thread = try std.Thread.spawn(.{}, listenerWorker, .{self});
        self.beacon_thread = try std.Thread.spawn(.{}, broadcasterWorker, .{self});
    }

    fn broadcasterWorker(self: *DiscoveryBeacon) void {
        const sock = std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM, 0) catch return;
        defer std.posix.close(sock);

        // Enable SO_BROADCAST
        const enable: c_int = 1;
        _ = std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.BROADCAST, std.mem.asBytes(&enable)) catch return;

        var broadcast_addr = std.net.Address.parseIp4("255.255.255.255", self.broadcast_port) catch return;

        var node_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.sync_mgr.engine.node_id)}) catch "";

        while (self.running) {
            var msg_buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &msg_buf,
                "STRATADB:BEACON:v1:{s}:{s}:{d}",
                .{ node_hex, self.sync_mgr.engine.desktop_name, self.sync_mgr.listen_port },
            ) catch continue;

            _ = std.posix.sendto(
                sock,
                msg,
                0,
                &broadcast_addr.any,
                broadcast_addr.getOsSockLen(),
            ) catch {};

            // Broadcast every 3 seconds
            std.time.sleep(3 * std.time.ns_per_s);
        }
    }

    fn listenerWorker(self: *DiscoveryBeacon) void {
        const sock = std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM, 0) catch return;
        defer std.posix.close(sock);

        // Reuse port/addr
        const enable: c_int = 1;
        _ = std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, std.mem.asBytes(&enable)) catch {};
        _ = std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, std.mem.asBytes(&enable)) catch {};

        var bind_addr = std.net.Address.parseIp4("0.0.0.0", self.broadcast_port) catch return;
        std.posix.bind(sock, &bind_addr.any, bind_addr.getOsSockLen()) catch return;

        var buf: [512]u8 = undefined;
        var src_addr: std.posix.sockaddr.in = undefined;
        var src_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);

        var my_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&my_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.sync_mgr.engine.node_id)}) catch "";

        while (self.running) {
            const n = std.posix.recvfrom(
                sock,
                &buf,
                0,
                @ptrCast(&src_addr),
                &src_len,
            ) catch continue;

            if (n <= 0) continue;
            const packet = buf[0..n];

            if (!std.mem.startsWith(u8, packet, "STRATADB:BEACON:v1:")) continue;

            var it = std.mem.splitScalar(u8, packet, ':');
            _ = it.next(); // STRATADB
            _ = it.next(); // BEACON
            _ = it.next(); // v1
            const peer_node_hex = it.next() orelse continue;
            _ = it.next(); // desktop_name
            const port_str = it.next() orelse continue;

            // Ignore our own beacon
            if (std.mem.eql(u8, peer_node_hex, &my_hex)) continue;

            const tcp_port = std.fmt.parseInt(u16, port_str, 10) catch continue;

            // Convert sender IP to string
            const ip_bytes = std.mem.asBytes(&src_addr.addr);
            var ip_buf: [64]u8 = undefined;
            const sender_ip = std.fmt.bufPrint(&ip_buf, "{d}.{d}.{d}.{d}", .{
                ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3],
            }) catch continue;

            // Check if already connected
            var already_connected = false;
            self.sync_mgr.peers_mutex.lock();
            for (self.sync_mgr.active_peers.items) |peer| {
                var phex: [32]u8 = undefined;
                _ = std.fmt.bufPrint(&phex, "{s}", .{std.fmt.fmtSliceHexLower(&peer.node_id)}) catch "";
                if (std.mem.eql(u8, &phex, peer_node_hex)) {
                    already_connected = true;
                    break;
                }
            }
            self.sync_mgr.peers_mutex.unlock();

            if (!already_connected) {
                // Auto-connect to newly discovered desktop peer!
                _ = self.sync_mgr.connectToPeer(sender_ip, tcp_port) catch {};
            }
        }
    }
};
