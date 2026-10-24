const std = @import("std");
const types = @import("../types.zig");
const Column = types.Column;
const Value = types.Value;

pub const AggregateType = enum {
    count,
    sum,
    avg,
    min,
    max,
};

pub const SelectItem = struct {
    star: bool = false,
    table_name: ?[]const u8 = null,
    column_name: ?[]const u8 = null,
    aggregate: ?AggregateType = null,
    alias: ?[]const u8 = null,
};

pub const BinaryOp = enum {
    eq,
    neq,
    lt,
    lte,
    gt,
    gte,
    and_op,
    or_op,
};

pub const ColumnRef = struct {
    table_name: ?[]const u8 = null,
    column_name: []const u8,
};

pub const Expr = union(enum) {
    literal: Value,
    column: ColumnRef,
    binary: struct {
        left: *Expr,
        op: BinaryOp,
        right: *Expr,
    },

    pub fn deinit(self: *Expr, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .literal => |v| v.deinit(allocator),
            .binary => |b| {
                b.left.deinit(allocator);
                allocator.destroy(b.left);
                b.right.deinit(allocator);
                allocator.destroy(b.right);
            },
            .column => {},
        }
    }
};

pub const JoinClause = struct {
    join_table: []const u8,
    left_col: []const u8,
    right_col: []const u8,
};

pub const OrderBy = struct {
    column_name: []const u8,
    ascending: bool = true,
};

pub const Assignment = struct {
    column_name: []const u8,
    value: Value,
};

pub const CreateTableStmt = struct {
    table_name: []const u8,
    columns: []Column,
};

pub const DropTableStmt = struct {
    table_name: []const u8,
};

pub const InsertStmt = struct {
    table_name: []const u8,
    columns: ?[][]const u8 = null,
    values: []Value,
};

pub const SelectStmt = struct {
    items: []SelectItem,
    from_table: []const u8,
    join: ?JoinClause = null,
    where: ?*Expr = null,
    order_by: ?OrderBy = null,
    limit: ?usize = null,
};

pub const UpdateStmt = struct {
    table_name: []const u8,
    assignments: []Assignment,
    where: ?*Expr = null,
};

pub const DeleteStmt = struct {
    table_name: []const u8,
    where: ?*Expr = null,
};

pub const SyncConnectStmt = struct {
    address: []const u8,
};

pub const AlterAction = union(enum) {
    add_column: Column,
    drop_column: []const u8,
    modify_column: Column,
    rename_column: struct {
        old_name: []const u8,
        new_name: []const u8,
    },
    drop_primary_key: void,
    add_primary_key: []const u8,
};

pub const AlterTableStmt = struct {
    table_name: []const u8,
    action: AlterAction,
};

pub const Statement = union(enum) {
    create_table: CreateTableStmt,
    alter_table: AlterTableStmt,
    drop_table: DropTableStmt,
    insert: InsertStmt,
    select: SelectStmt,
    update: UpdateStmt,
    delete: DeleteStmt,
    show_tables: void,
    describe: []const u8,
    peers: void,
    sync_connect: SyncConnectStmt,
    status: void,
    help: void,
    exit: void,
};
