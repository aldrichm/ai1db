const std = @import("std");

/// Hybrid Logical Clock (HLC)
/// Provides causality tracking and deterministic total ordering across distributed desktop nodes.
pub const HLC = struct {
    physical_time: i64, // Unix timestamp in milliseconds
    logical: u32,       // Monotonic counter for events within the same physical millisecond
    node_id: [16]u8,    // 16-byte node UUID for deterministic tie-breaking

    pub const zero = HLC{
        .physical_time = 0,
        .logical = 0,
        .node_id = [_]u8{0} ** 16,
    };

    pub fn init(node_id: [16]u8) HLC {
        return HLC{
            .physical_time = std.time.milliTimestamp(),
            .logical = 0,
            .node_id = node_id,
        };
    }

    /// Advances the HLC for a new local write event.
    pub fn now(self: *HLC) HLC {
        const pt = std.time.milliTimestamp();
        if (pt > self.physical_time) {
            self.physical_time = pt;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        return self.*;
    }

    /// Updates the local HLC upon receiving an event from a remote peer node.
    pub fn update(self: *HLC, remote: HLC) HLC {
        const pt = std.time.milliTimestamp();
        const max_pt = @max(pt, @max(self.physical_time, remote.physical_time));

        if (max_pt == self.physical_time and max_pt == remote.physical_time) {
            self.logical = @max(self.logical, remote.logical) + 1;
        } else if (max_pt == self.physical_time) {
            self.logical += 1;
        } else if (max_pt == remote.physical_time) {
            self.logical = remote.logical + 1;
        } else {
            self.logical = 0;
        }
        self.physical_time = max_pt;
        return self.*;
    }

    /// Total deterministic comparison:
    /// 1. Compare physical_time
    /// 2. If equal, compare logical counter
    /// 3. If equal, compare node_id
    pub fn compare(a: HLC, b: HLC) std.math.Order {
        if (a.physical_time != b.physical_time) {
            return std.math.order(a.physical_time, b.physical_time);
        }
        if (a.logical != b.logical) {
            return std.math.order(a.logical, b.logical);
        }
        return std.mem.order(u8, &a.node_id, &b.node_id);
    }

    pub fn isEqual(a: HLC, b: HLC) bool {
        return a.compare(b) == .eq;
    }

    pub fn isGreaterThan(a: HLC, b: HLC) bool {
        return a.compare(b) == .gt;
    }

    pub fn serialize(self: HLC, writer: anytype) !void {
        try writer.writeInt(i64, self.physical_time, .little);
        try writer.writeInt(u32, self.logical, .little);
        try writer.writeAll(&self.node_id);
    }

    pub fn deserialize(reader: anytype) !HLC {
        const pt = try reader.readInt(i64, .little);
        const log = try reader.readInt(u32, .little);
        var nid: [16]u8 = undefined;
        try reader.readNoEof(&nid);
        return HLC{
            .physical_time = pt,
            .logical = log,
            .node_id = nid,
        };
    }

    pub fn format(
        self: HLC,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        var node_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.node_id)}) catch "";
        try writer.print("{d}:{d}:{s}", .{ self.physical_time, self.logical, node_hex[0..8] });
    }

    pub fn toStringAlloc(self: HLC, allocator: std.mem.Allocator) ![]const u8 {
        var node_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.node_id)}) catch "";
        return try std.fmt.allocPrint(allocator, "{d}:{d}:{s}", .{ self.physical_time, self.logical, node_hex[0..8] });
    }
};
