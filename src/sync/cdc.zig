const std = @import("std");
const types = @import("../types.zig");
const Value = types.Value;
const Row = types.Row;
const Schema = types.Schema;
const HLC = @import("hlc.zig").HLC;

pub const Operation = enum(u8) {
    insert = 1,
    update = 2,
    delete = 3,

    pub fn name(self: Operation) []const u8 {
        return switch (self) {
            .insert => "INSERT",
            .update => "UPDATE",
            .delete => "DELETE",
        };
    }
};

pub const ChangeRecord = struct {
    table_name: []const u8,
    primary_key: Value,
    op: Operation,
    hlc: HLC,
    origin_node: [16]u8,
    row: ?Row = null, // null for delete

    pub fn clone(self: ChangeRecord, allocator: std.mem.Allocator) !ChangeRecord {
        return ChangeRecord{
            .table_name = try allocator.dupe(u8, self.table_name),
            .primary_key = try self.primary_key.clone(allocator),
            .op = self.op,
            .hlc = self.hlc,
            .origin_node = self.origin_node,
            .row = if (self.row) |r| try r.clone(allocator) else null,
        };
    }

    pub fn deinit(self: ChangeRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        self.primary_key.deinit(allocator);
        if (self.row) |r| {
            r.deinit(allocator);
        }
    }

    pub fn serialize(self: ChangeRecord, writer: anytype) !void {
        try writer.writeInt(u16, @intCast(self.table_name.len), .little);
        try writer.writeAll(self.table_name);
        try self.primary_key.serialize(writer);
        try writer.writeByte(@intFromEnum(self.op));
        try self.hlc.serialize(writer);
        try writer.writeAll(&self.origin_node);
        if (self.row) |r| {
            try writer.writeByte(1);
            try r.serialize(writer);
        } else {
            try writer.writeByte(0);
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !ChangeRecord {
        const name_len = try reader.readInt(u16, .little);
        const name_buf = try allocator.alloc(u8, name_len);
        errdefer allocator.free(name_buf);
        try reader.readNoEof(name_buf);

        const pk = try Value.deserialize(allocator, reader);
        errdefer pk.deinit(allocator);

        const op_byte = try reader.readByte();
        const op: Operation = @enumFromInt(op_byte);
        const hlc = try HLC.deserialize(reader);
        var origin: [16]u8 = undefined;
        try reader.readNoEof(&origin);

        const has_row = try reader.readByte();
        var row: ?Row = null;
        if (has_row != 0) {
            row = try Row.deserialize(allocator, reader);
        }

        return ChangeRecord{
            .table_name = name_buf,
            .primary_key = pk,
            .op = op,
            .hlc = hlc,
            .origin_node = origin,
            .row = row,
        };
    }
};

pub const ChangeLog = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(ChangeRecord),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) ChangeLog {
        return ChangeLog{
            .allocator = allocator,
            .entries = std.ArrayList(ChangeRecord).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *ChangeLog) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            entry.deinit(self.allocator);
        }
        self.entries.deinit();
    }

    pub fn append(self: *ChangeLog, record: ChangeRecord) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const cloned = try record.clone(self.allocator);
        try self.entries.append(cloned);
    }

    pub fn getChangesSince(self: *ChangeLog, since_hlc: ?HLC, allocator: std.mem.Allocator) ![]ChangeRecord {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = std.ArrayList(ChangeRecord).init(allocator);
        errdefer {
            for (result.items) |item| item.deinit(allocator);
            result.deinit();
        }

        for (self.entries.items) |entry| {
            if (since_hlc) |sh| {
                if (entry.hlc.isGreaterThan(sh)) {
                    const cloned = try entry.clone(allocator);
                    try result.append(cloned);
                }
            } else {
                const cloned = try entry.clone(allocator);
                try result.append(cloned);
            }
        }

        return result.toOwnedSlice();
    }

    pub fn getLatestHLC(self: *ChangeLog) HLC {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return HLC.zero;
        return self.entries.items[self.entries.items.len - 1].hlc;
    }

    pub fn count(self: *ChangeLog) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.entries.items.len;
    }
};
