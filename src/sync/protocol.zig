const std = @import("std");
const types = @import("../types.zig");
const HLC = @import("hlc.zig").HLC;
const cdc = @import("cdc.zig");
const ChangeRecord = cdc.ChangeRecord;

pub const MAGIC_SYNC: [4]u8 = [_]u8{ 'S', 'D', 'B', 'S' }; // StrataDB Sync
pub const PROTOCOL_VERSION: u16 = 1;

pub const MessageType = enum(u8) {
    hello = 1,
    hello_ack = 2,
    sync_request = 3,
    sync_response = 4,
    live_mutation = 5,
    ping = 6,
    pong = 7,
};

pub const HelloMessage = struct {
    node_id: [16]u8,
    desktop_name: []const u8,
    current_hlc: HLC,
    listen_port: u16,

    pub fn serialize(self: HelloMessage, writer: anytype) !void {
        try writer.writeAll(&self.node_id);
        try writer.writeInt(u16, @intCast(self.desktop_name.len), .little);
        try writer.writeAll(self.desktop_name);
        try self.current_hlc.serialize(writer);
        try writer.writeInt(u16, self.listen_port, .little);
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !HelloMessage {
        var nid: [16]u8 = undefined;
        try reader.readNoEof(&nid);
        const name_len = try reader.readInt(u16, .little);
        const name_buf = try allocator.alloc(u8, name_len);
        errdefer allocator.free(name_buf);
        try reader.readNoEof(name_buf);
        const hlc = try HLC.deserialize(reader);
        const port = try reader.readInt(u16, .little);

        return HelloMessage{
            .node_id = nid,
            .desktop_name = name_buf,
            .current_hlc = hlc,
            .listen_port = port,
        };
    }
};

pub const SyncRequestMessage = struct {
    since_hlc: ?HLC,

    pub fn serialize(self: SyncRequestMessage, writer: anytype) !void {
        if (self.since_hlc) |sh| {
            try writer.writeByte(1);
            try sh.serialize(writer);
        } else {
            try writer.writeByte(0);
        }
    }

    pub fn deserialize(reader: anytype) !SyncRequestMessage {
        const has_hlc = try reader.readByte();
        var sh: ?HLC = null;
        if (has_hlc != 0) {
            sh = try HLC.deserialize(reader);
        }
        return SyncRequestMessage{ .since_hlc = sh };
    }
};

pub const Frame = struct {
    msg_type: MessageType,
    payload: []const u8,

    pub fn write(writer: anytype, msg_type: MessageType, payload: []const u8) !void {
        try writer.writeAll(&MAGIC_SYNC);
        try writer.writeInt(u16, PROTOCOL_VERSION, .little);
        try writer.writeByte(@intFromEnum(msg_type));
        try writer.writeInt(u32, @intCast(payload.len), .little);
        try writer.writeAll(payload);
    }

    pub fn read(allocator: std.mem.Allocator, reader: anytype) !Frame {
        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!std.mem.eql(u8, &magic, &MAGIC_SYNC)) {
            return error.InvalidSyncProtocolMagic;
        }

        const ver = try reader.readInt(u16, .little);
        if (ver != PROTOCOL_VERSION) {
            return error.UnsupportedSyncVersion;
        }

        const type_byte = try reader.readByte();
        const msg_type: MessageType = @enumFromInt(type_byte);
        const len = try reader.readInt(u32, .little);

        const payload = try allocator.alloc(u8, len);
        errdefer allocator.free(payload);
        try reader.readNoEof(payload);

        return Frame{
            .msg_type = msg_type,
            .payload = payload,
        };
    }
};
