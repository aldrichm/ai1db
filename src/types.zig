const std = @import("std");

pub const DataType = enum(u8) {
    integer = 1,
    real = 2,
    text = 3,
    boolean = 4,
    timestamp = 5,
    blob = 6,

    pub fn fromString(str: []const u8) ?DataType {
        if (std.ascii.eqlIgnoreCase(str, "INT") or
            std.ascii.eqlIgnoreCase(str, "INTEGER") or
            std.ascii.eqlIgnoreCase(str, "BIGINT"))
        {
            return .integer;
        } else if (std.ascii.eqlIgnoreCase(str, "REAL") or
            std.ascii.eqlIgnoreCase(str, "FLOAT") or
            std.ascii.eqlIgnoreCase(str, "DOUBLE"))
        {
            return .real;
        } else if (std.ascii.eqlIgnoreCase(str, "TEXT") or
            std.ascii.eqlIgnoreCase(str, "VARCHAR") or
            std.ascii.eqlIgnoreCase(str, "STRING"))
        {
            return .text;
        } else if (std.ascii.eqlIgnoreCase(str, "BOOL") or
            std.ascii.eqlIgnoreCase(str, "BOOLEAN"))
        {
            return .boolean;
        } else if (std.ascii.eqlIgnoreCase(str, "TIMESTAMP") or
            std.ascii.eqlIgnoreCase(str, "DATETIME"))
        {
            return .timestamp;
        } else if (std.ascii.eqlIgnoreCase(str, "BLOB") or
            std.ascii.eqlIgnoreCase(str, "BYTES"))
        {
            return .blob;
        }
        return null;
    }

    pub fn name(self: DataType) []const u8 {
        return switch (self) {
            .integer => "INTEGER",
            .real => "REAL",
            .text => "TEXT",
            .boolean => "BOOLEAN",
            .timestamp => "TIMESTAMP",
            .blob => "BLOB",
        };
    }
};

pub const ValueTag = enum(u8) {
    null_val = 0,
    integer = 1,
    real = 2,
    text = 3,
    boolean = 4,
    timestamp = 5,
    blob = 6,
};

pub const Value = union(ValueTag) {
    null_val: void,
    integer: i64,
    real: f64,
    text: []const u8,
    boolean: bool,
    timestamp: i64,
    blob: []const u8,

    pub fn clone(self: Value, allocator: std.mem.Allocator) !Value {
        return switch (self) {
            .null_val => .null_val,
            .integer => |i| Value{ .integer = i },
            .real => |r| Value{ .real = r },
            .text => |t| Value{ .text = try allocator.dupe(u8, t) },
            .boolean => |b| Value{ .boolean = b },
            .timestamp => |ts| Value{ .timestamp = ts },
            .blob => |b| Value{ .blob = try allocator.dupe(u8, b) },
        };
    }

    pub fn deinit(self: Value, allocator: std.mem.Allocator) void {
        switch (self) {
            .text => |t| allocator.free(t),
            .blob => |b| allocator.free(b),
            else => {},
        }
    }

    pub fn isNull(self: Value) bool {
        return self == .null_val;
    }

    pub fn getDataType(self: Value) ?DataType {
        return switch (self) {
            .null_val => null,
            .integer => .integer,
            .real => .real,
            .text => .text,
            .boolean => .boolean,
            .timestamp => .timestamp,
            .blob => .blob,
        };
    }

    pub fn compare(self: Value, other: Value) std.math.Order {
        if (self == .null_val and other == .null_val) return .eq;
        if (self == .null_val) return .lt;
        if (other == .null_val) return .gt;

        switch (self) {
            .integer => |a| {
                switch (other) {
                    .integer => |b| return std.math.order(a, b),
                    .real => |b| return std.math.order(@as(f64, @floatFromInt(a)), b),
                    else => return std.math.order(@intFromEnum(self), @intFromEnum(other)),
                }
            },
            .real => |a| {
                switch (other) {
                    .real => |b| return std.math.order(a, b),
                    .integer => |b| return std.math.order(a, @as(f64, @floatFromInt(b))),
                    else => return std.math.order(@intFromEnum(self), @intFromEnum(other)),
                }
            },
            .text => |a| {
                if (other == .text) {
                    const cmp = std.mem.order(u8, a, other.text);
                    return cmp;
                }
                return std.math.order(@intFromEnum(self), @intFromEnum(other));
            },
            .boolean => |a| {
                if (other == .boolean) {
                    const a_int: u8 = if (a) 1 else 0;
                    const b_int: u8 = if (other.boolean) 1 else 0;
                    return std.math.order(a_int, b_int);
                }
                return std.math.order(@intFromEnum(self), @intFromEnum(other));
            },
            .timestamp => |a| {
                if (other == .timestamp) {
                    return std.math.order(a, other.timestamp);
                }
                return std.math.order(@intFromEnum(self), @intFromEnum(other));
            },
            .blob => |a| {
                if (other == .blob) {
                    return std.mem.order(u8, a, other.blob);
                }
                return std.math.order(@intFromEnum(self), @intFromEnum(other));
            },
            .null_val => unreachable,
        }
    }

    pub fn isEqual(self: Value, other: Value) bool {
        return self.compare(other) == .eq;
    }

    pub fn toStringAlloc(self: Value, allocator: std.mem.Allocator) ![]const u8 {
        switch (self) {
            .null_val => return try allocator.dupe(u8, "NULL"),
            .integer => |i| return try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .real => |r| return try std.fmt.allocPrint(allocator, "{d:.4}", .{r}),
            .text => |t| return try allocator.dupe(u8, t),
            .boolean => |b| return try allocator.dupe(u8, if (b) "true" else "false"),
            .timestamp => |ts| return try std.fmt.allocPrint(allocator, "{d}", .{ts}),
            .blob => |b| return try std.fmt.allocPrint(allocator, "<blob {d} bytes>", .{b.len}),
        }
    }

    pub fn serialize(self: Value, writer: anytype) !void {
        try writer.writeByte(@intFromEnum(self));
        switch (self) {
            .null_val => {},
            .integer => |i| try writer.writeInt(i64, i, .little),
            .real => |r| try writer.writeInt(u64, @bitCast(r), .little),
            .text => |t| {
                try writer.writeInt(u32, @intCast(t.len), .little);
                try writer.writeAll(t);
            },
            .boolean => |b| try writer.writeByte(if (b) 1 else 0),
            .timestamp => |ts| try writer.writeInt(i64, ts, .little),
            .blob => |b| {
                try writer.writeInt(u32, @intCast(b.len), .little);
                try writer.writeAll(b);
            },
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !Value {
        const tag_byte = try reader.readByte();
        const tag: ValueTag = @enumFromInt(tag_byte);
        return switch (tag) {
            .null_val => .null_val,
            .integer => Value{ .integer = try reader.readInt(i64, .little) },
            .real => {
                const bits = try reader.readInt(u64, .little);
                return Value{ .real = @bitCast(bits) };
            },
            .text => {
                const len = try reader.readInt(u32, .little);
                const buf = try allocator.alloc(u8, len);
                errdefer allocator.free(buf);
                try reader.readNoEof(buf);
                return Value{ .text = buf };
            },
            .boolean => {
                const b = try reader.readByte();
                return Value{ .boolean = (b != 0) };
            },
            .timestamp => Value{ .timestamp = try reader.readInt(i64, .little) },
            .blob => {
                const len = try reader.readInt(u32, .little);
                const buf = try allocator.alloc(u8, len);
                errdefer allocator.free(buf);
                try reader.readNoEof(buf);
                return Value{ .blob = buf };
            },
        };
    }
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,
    primary_key: bool = false,
    nullable: bool = true,

    pub fn clone(self: Column, allocator: std.mem.Allocator) !Column {
        return Column{
            .name = try allocator.dupe(u8, self.name),
            .data_type = self.data_type,
            .primary_key = self.primary_key,
            .nullable = self.nullable,
        };
    }

    pub fn deinit(self: Column, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }

    pub fn serialize(self: Column, writer: anytype) !void {
        try writer.writeInt(u16, @intCast(self.name.len), .little);
        try writer.writeAll(self.name);
        try writer.writeByte(@intFromEnum(self.data_type));
        try writer.writeByte(if (self.primary_key) 1 else 0);
        try writer.writeByte(if (self.nullable) 1 else 0);
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !Column {
        const name_len = try reader.readInt(u16, .little);
        const name_buf = try allocator.alloc(u8, name_len);
        errdefer allocator.free(name_buf);
        try reader.readNoEof(name_buf);

        const type_byte = try reader.readByte();
        const pk_byte = try reader.readByte();
        const null_byte = try reader.readByte();

        return Column{
            .name = name_buf,
            .data_type = @enumFromInt(type_byte),
            .primary_key = (pk_byte != 0),
            .nullable = (null_byte != 0),
        };
    }
};

pub const Schema = struct {
    columns: []Column,

    pub fn init(allocator: std.mem.Allocator, cols: []const Column) !Schema {
        const columns = try allocator.alloc(Column, cols.len);
        errdefer allocator.free(columns);
        for (cols, 0..) |c, i| {
            columns[i] = try c.clone(allocator);
        }
        return Schema{ .columns = columns };
    }

    pub fn clone(self: Schema, allocator: std.mem.Allocator) !Schema {
        const cols = try allocator.alloc(Column, self.columns.len);
        errdefer allocator.free(cols);
        for (self.columns, 0..) |c, i| {
            cols[i] = try c.clone(allocator);
        }
        return Schema{ .columns = cols };
    }

    pub fn deinit(self: Schema, allocator: std.mem.Allocator) void {
        for (self.columns) |c| {
            c.deinit(allocator);
        }
        allocator.free(self.columns);
    }

    pub fn getColumnIndex(self: Schema, col_name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, col_name)) {
                return i;
            }
        }
        return null;
    }

    pub fn getPrimaryKeyIndex(self: Schema) ?usize {
        for (self.columns, 0..) |col, i| {
            if (col.primary_key) return i;
        }
        return null;
    }

    pub fn serialize(self: Schema, writer: anytype) !void {
        try writer.writeInt(u16, @intCast(self.columns.len), .little);
        for (self.columns) |col| {
            try col.serialize(writer);
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !Schema {
        const count = try reader.readInt(u16, .little);
        const cols = try allocator.alloc(Column, count);
        errdefer allocator.free(cols);
        var i: usize = 0;
        errdefer {
            for (cols[0..i]) |c| c.deinit(allocator);
        }
        while (i < count) : (i += 1) {
            cols[i] = try Column.deserialize(allocator, reader);
        }
        return Schema{ .columns = cols };
    }
};

pub const Row = struct {
    values: []Value,

    pub fn init(allocator: std.mem.Allocator, count: usize) !Row {
        const vals = try allocator.alloc(Value, count);
        for (vals) |*v| {
            v.* = .null_val;
        }
        return Row{ .values = vals };
    }

    pub fn clone(self: Row, allocator: std.mem.Allocator) !Row {
        const vals = try allocator.alloc(Value, self.values.len);
        errdefer allocator.free(vals);
        for (self.values, 0..) |v, i| {
            vals[i] = try v.clone(allocator);
        }
        return Row{ .values = vals };
    }

    pub fn deinit(self: Row, allocator: std.mem.Allocator) void {
        for (self.values) |v| {
            v.deinit(allocator);
        }
        allocator.free(self.values);
    }

    pub fn serialize(self: Row, writer: anytype) !void {
        try writer.writeInt(u16, @intCast(self.values.len), .little);
        for (self.values) |val| {
            try val.serialize(writer);
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !Row {
        const count = try reader.readInt(u16, .little);
        const vals = try allocator.alloc(Value, count);
        errdefer allocator.free(vals);
        var i: usize = 0;
        errdefer {
            for (vals[0..i]) |v| v.deinit(allocator);
        }
        while (i < count) : (i += 1) {
            vals[i] = try Value.deserialize(allocator, reader);
        }
        return Row{ .values = vals };
    }
};
