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
    table_sync = 8,
    ddl_drop = 9,
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

pub const ColumnDesc = struct {
    col_name: []const u8,
    desc: []const u8,
};

pub const RowSnapshot = struct {
    row: types.Row,
    hlc: HLC,
};

pub const TombstoneSnapshot = struct {
    pk_str: []const u8,
    hlc: HLC,
};

pub const TableSnapshot = struct {
    name: []const u8,
    schema: types.Schema,
    description: []const u8,
    column_descriptions: []ColumnDesc,
    next_auto_inc: i64,
    rows: []RowSnapshot,
    tombstones: []TombstoneSnapshot,

    pub fn deinit(self: TableSnapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        var s = self.schema;
        s.deinit(allocator);
        allocator.free(self.description);
        for (self.column_descriptions) |cd| {
            allocator.free(cd.col_name);
            allocator.free(cd.desc);
        }
        allocator.free(self.column_descriptions);
        for (self.rows) |r| {
            r.row.deinit(allocator);
        }
        allocator.free(self.rows);
        for (self.tombstones) |t| {
            allocator.free(t.pk_str);
        }
        allocator.free(self.tombstones);
    }

    pub fn serialize(self: TableSnapshot, writer: anytype) !void {
        try writer.writeInt(u16, @intCast(self.name.len), .little);
        try writer.writeAll(self.name);
        try self.schema.serialize(writer);
        try writer.writeInt(u16, @intCast(self.description.len), .little);
        try writer.writeAll(self.description);
        try writer.writeInt(u16, @intCast(self.column_descriptions.len), .little);
        for (self.column_descriptions) |cd| {
            try writer.writeInt(u16, @intCast(cd.col_name.len), .little);
            try writer.writeAll(cd.col_name);
            try writer.writeInt(u16, @intCast(cd.desc.len), .little);
            try writer.writeAll(cd.desc);
        }
        try writer.writeInt(i64, self.next_auto_inc, .little);
        try writer.writeInt(u32, @intCast(self.rows.len), .little);
        for (self.rows) |r| {
            try r.row.serialize(writer);
            try r.hlc.serialize(writer);
        }
        try writer.writeInt(u32, @intCast(self.tombstones.len), .little);
        for (self.tombstones) |t| {
            try writer.writeInt(u16, @intCast(t.pk_str.len), .little);
            try writer.writeAll(t.pk_str);
            try t.hlc.serialize(writer);
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !TableSnapshot {
        const name_len = try reader.readInt(u16, .little);
        const name = try allocator.alloc(u8, name_len);
        errdefer allocator.free(name);
        try reader.readNoEof(name);

        const schema = try types.Schema.deserializeVersion(allocator, reader, 2);
        errdefer {
            var s = schema;
            s.deinit(allocator);
        }

        const desc_len = try reader.readInt(u16, .little);
        const desc = try allocator.alloc(u8, desc_len);
        errdefer allocator.free(desc);
        try reader.readNoEof(desc);

        const cd_count = try reader.readInt(u16, .little);
        const col_descs = try allocator.alloc(ColumnDesc, cd_count);
        errdefer allocator.free(col_descs);
        var cd_i: usize = 0;
        errdefer {
            for (col_descs[0..cd_i]) |cd| {
                allocator.free(cd.col_name);
                allocator.free(cd.desc);
            }
        }
        while (cd_i < cd_count) : (cd_i += 1) {
            const cn_len = try reader.readInt(u16, .little);
            const cn = try allocator.alloc(u8, cn_len);
            errdefer allocator.free(cn);
            try reader.readNoEof(cn);

            const cd_len = try reader.readInt(u16, .little);
            const cd_str = try allocator.alloc(u8, cd_len);
            errdefer allocator.free(cd_str);
            try reader.readNoEof(cd_str);

            col_descs[cd_i] = ColumnDesc{
                .col_name = cn,
                .desc = cd_str,
            };
        }

        const next_auto_inc = try reader.readInt(i64, .little);

        const row_count = try reader.readInt(u32, .little);
        const rows = try allocator.alloc(RowSnapshot, row_count);
        errdefer allocator.free(rows);
        var r_i: usize = 0;
        errdefer {
            for (rows[0..r_i]) |r| {
                r.row.deinit(allocator);
            }
        }
        while (r_i < row_count) : (r_i += 1) {
            const row = try types.Row.deserialize(allocator, reader);
            const hlc = try HLC.deserialize(reader);
            rows[r_i] = RowSnapshot{
                .row = row,
                .hlc = hlc,
            };
        }

        const tomb_count = try reader.readInt(u32, .little);
        const tombstones = try allocator.alloc(TombstoneSnapshot, tomb_count);
        errdefer allocator.free(tombstones);
        var t_i: usize = 0;
        errdefer {
            for (tombstones[0..t_i]) |t| {
                allocator.free(t.pk_str);
            }
        }
        while (t_i < tomb_count) : (t_i += 1) {
            const pk_len = try reader.readInt(u16, .little);
            const pk_buf = try allocator.alloc(u8, pk_len);
            errdefer allocator.free(pk_buf);
            try reader.readNoEof(pk_buf);
            const hlc = try HLC.deserialize(reader);
            tombstones[t_i] = TombstoneSnapshot{
                .pk_str = pk_buf,
                .hlc = hlc,
            };
        }

        return TableSnapshot{
            .name = name,
            .schema = schema,
            .description = desc,
            .column_descriptions = col_descs,
            .next_auto_inc = next_auto_inc,
            .rows = rows,
            .tombstones = tombstones,
        };
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
