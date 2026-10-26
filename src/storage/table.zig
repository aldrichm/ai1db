const std = @import("std");
const types = @import("../types.zig");
const Value = types.Value;
const Column = types.Column;
const Schema = types.Schema;
const Row = types.Row;
const HLC = @import("../sync/hlc.zig").HLC;
const cdc = @import("../sync/cdc.zig");
const ChangeRecord = cdc.ChangeRecord;
const Operation = cdc.Operation;
const crdt = @import("../sync/crdt.zig");
const RowState = crdt.RowState;
const ConflictResolution = crdt.ConflictResolution;

pub const Table = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    schema: Schema,
    rows: std.ArrayList(Row),
    /// Primary key string -> index in rows array
    pk_index: std.StringHashMap(usize),
    /// Primary key string -> HLC of current active row
    row_hlcs: std.StringHashMap(HLC),
    /// Primary key string -> HLC of tombstone (deleted row)
    tombstones: std.StringHashMap(HLC),
    next_auto_inc: i64 = 1,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, schema: Schema) !*Table {
        const table = try allocator.create(Table);
        errdefer allocator.destroy(table);

        const duped_name = try allocator.dupe(u8, name);
        errdefer allocator.free(duped_name);

        const cloned_schema = try schema.clone(allocator);
        errdefer cloned_schema.deinit(allocator);

        table.* = Table{
            .allocator = allocator,
            .name = duped_name,
            .schema = cloned_schema,
            .rows = std.ArrayList(Row).init(allocator),
            .pk_index = std.StringHashMap(usize).init(allocator),
            .row_hlcs = std.StringHashMap(HLC).init(allocator),
            .tombstones = std.StringHashMap(HLC).init(allocator),
            .next_auto_inc = 1,
            .mutex = std.Thread.Mutex{},
        };
        return table;
    }

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |row| {
            row.deinit(self.allocator);
        }
        self.rows.deinit();

        var pk_it = self.pk_index.keyIterator();
        while (pk_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.pk_index.deinit();

        var hlc_it = self.row_hlcs.keyIterator();
        while (hlc_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.row_hlcs.deinit();

        var tomb_it = self.tombstones.keyIterator();
        while (tomb_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.tombstones.deinit();

        self.schema.deinit(self.allocator);
        self.allocator.free(self.name);
        self.allocator.destroy(self);
    }

    pub fn getPrimaryKeyString(self: *Table, pk_val: Value) ![]const u8 {
        return try pk_val.toStringAlloc(self.allocator);
    }

    pub fn getRowPrimaryKeyString(self: *Table, row: Row) !?[]const u8 {
        const pk_count = self.schema.getPrimaryKeyCount();
        if (pk_count == 0) return null;

        if (pk_count == 1) {
            const pk_idx = self.schema.getPrimaryKeyIndex().?;
            if (pk_idx >= row.values.len or row.values[pk_idx] == .null_val) {
                return error.PrimaryKeyCannotBeNull;
            }
            return try self.getPrimaryKeyString(row.values[pk_idx]);
        }

        // Composite primary key: concatenate with ASCII 0x1F unit separator
        var list = std.ArrayList(u8).init(self.allocator);
        errdefer list.deinit();

        var first = true;
        for (self.schema.columns, 0..) |col, idx| {
            if (!col.primary_key) continue;
            if (idx >= row.values.len or row.values[idx] == .null_val) {
                return error.PrimaryKeyCannotBeNull;
            }
            if (!first) {
                try list.append(0x1F);
            }
            first = false;

            const val_str = try self.getPrimaryKeyString(row.values[idx]);
            defer self.allocator.free(val_str);
            try list.appendSlice(val_str);
        }

        return try list.toOwnedSlice();
    }

    pub fn getRowPrimaryKey(self: *Table, row: Row) !Value {
        const pk_count = self.schema.getPrimaryKeyCount();
        if (pk_count == 1) {
            if (self.schema.getPrimaryKeyIndex()) |pk_idx| {
                if (pk_idx < row.values.len) {
                    return row.values[pk_idx];
                }
            }
        } else if (pk_count > 1) {
            if (try self.getRowPrimaryKeyString(row)) |pk_str| {
                return Value{ .text = pk_str };
            }
        }
        return Value{ .integer = 0 };
    }

    pub fn insertRow(self: *Table, row: Row, hlc: HLC) !void {
        const cloned_row = try row.clone(self.allocator);
        errdefer cloned_row.deinit(self.allocator);
        try self.insertRowDirect(cloned_row, hlc);
    }

    pub fn insertRowDirect(self: *Table, row: Row, hlc: HLC) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Auto-increment sequence resolution
        for (self.schema.columns, 0..) |col, i| {
            if (col.auto_increment and i < row.values.len) {
                if (row.values[i] == .null_val) {
                    row.values[i] = Value{ .integer = self.next_auto_inc };
                    self.next_auto_inc += 1;
                } else if (row.values[i] == .integer) {
                    if (row.values[i].integer >= self.next_auto_inc) {
                        self.next_auto_inc = row.values[i].integer + 1;
                    }
                }
            }
        }

        if (try self.getRowPrimaryKeyString(row)) |pk_str| {
            defer self.allocator.free(pk_str);

            if (self.pk_index.contains(pk_str)) {
                return error.DuplicatePrimaryKey;
            }

            // Remove from tombstones if it was previously deleted
            if (self.tombstones.fetchRemove(pk_str)) |entry| {
                self.allocator.free(entry.key);
            }

            const new_idx = self.rows.items.len;
            try self.rows.append(row);

            const key_for_map1 = try self.allocator.dupe(u8, pk_str);
            try self.pk_index.put(key_for_map1, new_idx);

            const key_for_map2 = try self.allocator.dupe(u8, pk_str);
            try self.row_hlcs.put(key_for_map2, hlc);
        } else {
            const new_idx = self.rows.items.len;
            try self.rows.append(row);

            var buf: [32]u8 = undefined;
            const synth_key = try std.fmt.bufPrint(&buf, "__row_{d}", .{new_idx + 1});
            const key_for_map = try self.allocator.dupe(u8, synth_key);
            try self.row_hlcs.put(key_for_map, hlc);
        }
    }

    pub fn updateRow(self: *Table, pk: Value, new_row: Row, hlc: HLC) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pk_str = try self.getPrimaryKeyString(pk);
        defer self.allocator.free(pk_str);

        const idx = self.pk_index.get(pk_str) orelse return error.RowNotFound;

        // Replace old row values
        self.rows.items[idx].deinit(self.allocator);
        self.rows.items[idx] = try new_row.clone(self.allocator);

        if (self.row_hlcs.getPtr(pk_str)) |ptr| {
            ptr.* = hlc;
        } else {
            const k = try self.allocator.dupe(u8, pk_str);
            try self.row_hlcs.put(k, hlc);
        }
    }

    pub fn deleteRow(self: *Table, pk: Value, hlc: HLC) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pk_str = try self.getPrimaryKeyString(pk);
        defer self.allocator.free(pk_str);

        const idx = self.pk_index.get(pk_str) orelse return false;

        // Clean up row
        self.rows.items[idx].deinit(self.allocator);

        // Swap with last row for O(1) removal, and update swapped row's pk_index
        const last_idx = self.rows.items.len - 1;
        if (idx != last_idx) {
            const swapped_row = self.rows.items[last_idx];
            self.rows.items[idx] = swapped_row;
            if (try self.getRowPrimaryKeyString(swapped_row)) |swapped_pk_str| {
                defer self.allocator.free(swapped_pk_str);
                if (self.pk_index.getPtr(swapped_pk_str)) |ptr| {
                    ptr.* = idx;
                }
            }
        }
        _ = self.rows.pop();

        // Remove from pk_index and row_hlcs
        if (self.pk_index.fetchRemove(pk_str)) |entry| {
            self.allocator.free(entry.key);
        }
        if (self.row_hlcs.fetchRemove(pk_str)) |entry| {
            self.allocator.free(entry.key);
        }

        // Add to tombstones
        const tomb_key = try self.allocator.dupe(u8, pk_str);
        try self.tombstones.put(tomb_key, hlc);

        return true;
    }

    pub fn updateRowByIndex(self: *Table, idx: usize, new_row: Row, hlc: HLC) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (idx >= self.rows.items.len) return error.RowNotFound;

        const old_pk_str_opt = try self.getRowPrimaryKeyString(self.rows.items[idx]);
        defer if (old_pk_str_opt) |s| self.allocator.free(s);
        const new_pk_str_opt = try self.getRowPrimaryKeyString(new_row);
        defer if (new_pk_str_opt) |s| self.allocator.free(s);

        if (old_pk_str_opt != null and new_pk_str_opt != null) {
            const old_pk_str = old_pk_str_opt.?;
            const new_pk_str = new_pk_str_opt.?;

            if (!std.mem.eql(u8, old_pk_str, new_pk_str)) {
                if (self.pk_index.contains(new_pk_str)) {
                    return error.DuplicatePrimaryKey;
                }
                if (self.pk_index.fetchRemove(old_pk_str)) |entry| {
                    self.allocator.free(entry.key);
                }
                const key_dup = try self.allocator.dupe(u8, new_pk_str);
                try self.pk_index.put(key_dup, idx);
            }
            if (self.row_hlcs.getPtr(new_pk_str)) |ptr| {
                ptr.* = hlc;
            } else {
                const k = try self.allocator.dupe(u8, new_pk_str);
                try self.row_hlcs.put(k, hlc);
            }
        }

        self.rows.items[idx].deinit(self.allocator);
        self.rows.items[idx] = try new_row.clone(self.allocator);
    }

    pub fn deleteRowByIndex(self: *Table, idx: usize, hlc: HLC) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (idx >= self.rows.items.len) return false;

        const pk_str_opt = try self.getRowPrimaryKeyString(self.rows.items[idx]);
        defer if (pk_str_opt) |s| self.allocator.free(s);

        self.rows.items[idx].deinit(self.allocator);

        const last_idx = self.rows.items.len - 1;
        if (idx != last_idx) {
            const swapped_row = self.rows.items[last_idx];
            self.rows.items[idx] = swapped_row;
            if (try self.getRowPrimaryKeyString(swapped_row)) |swapped_pk_str| {
                defer self.allocator.free(swapped_pk_str);
                if (self.pk_index.getPtr(swapped_pk_str)) |ptr| {
                    ptr.* = idx;
                }
            }
        }
        _ = self.rows.pop();

        if (pk_str_opt) |pk_str| {
            if (self.pk_index.fetchRemove(pk_str)) |entry| {
                self.allocator.free(entry.key);
            }
            if (self.row_hlcs.fetchRemove(pk_str)) |entry| {
                self.allocator.free(entry.key);
            }
            const tomb_key = try self.allocator.dupe(u8, pk_str);
            try self.tombstones.put(tomb_key, hlc);
        }

        return true;
    }

    pub fn getRowByPk(self: *Table, pk: Value) !?Row {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pk_str = try self.getPrimaryKeyString(pk);
        defer self.allocator.free(pk_str);

        if (self.pk_index.get(pk_str)) |idx| {
            return try self.rows.items[idx].clone(self.allocator);
        }
        return null;
    }

    pub fn clearPkIndexes(self: *Table) void {
        var pk_it = self.pk_index.keyIterator();
        while (pk_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.pk_index.clearRetainingCapacity();

        var hlc_it = self.row_hlcs.keyIterator();
        while (hlc_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.row_hlcs.clearRetainingCapacity();

        var tomb_it = self.tombstones.keyIterator();
        while (tomb_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.tombstones.clearRetainingCapacity();
    }

    pub fn dropPrimaryKeyInternal(self: *Table) void {
        self.clearPkIndexes();
        for (self.schema.columns) |*c| {
            c.primary_key = false;
        }
    }

    fn recreatePrimaryKeyFromMetadataInternal(self: *Table) !void {
        const has_pk = self.schema.hasPrimaryKey();

        // 1. If there is a PK column in metadata, validate all existing rows
        if (has_pk) {
            var seen = std.StringHashMap(void).init(self.allocator);
            defer {
                var it = seen.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                seen.deinit();
            }

            for (self.rows.items) |row| {
                const s = (try self.getRowPrimaryKeyString(row)) orelse return error.PrimaryKeyCannotBeNull;
                if (seen.contains(s)) {
                    self.allocator.free(s);
                    return error.DuplicatePrimaryKey;
                }
                try seen.put(s, {});
            }
        }

        // 2. Preserve existing row HLCs before dropping the PK
        const preserved_hlcs = try self.allocator.alloc(HLC, self.rows.items.len);
        defer self.allocator.free(preserved_hlcs);

        for (self.rows.items, 0..) |row, i| {
            if (try self.getRowPrimaryKeyString(row)) |old_pk_str| {
                defer self.allocator.free(old_pk_str);
                preserved_hlcs[i] = self.row_hlcs.get(old_pk_str) orelse HLC.zero;
            } else {
                var buf: [32]u8 = undefined;
                const synth_key = std.fmt.bufPrint(&buf, "__row_{d}", .{i + 1}) catch "";
                preserved_hlcs[i] = self.row_hlcs.get(synth_key) orelse HLC.zero;
            }
        }

        // 3. Drop the entire primary key indexes
        self.clearPkIndexes();

        // 4. Recreate primary key from metadata
        if (has_pk) {
            for (self.schema.columns) |*c| {
                if (c.primary_key) {
                    c.nullable = false;
                }
            }

            for (self.rows.items, 0..) |row, i| {
                const pk_str = (try self.getRowPrimaryKeyString(row)) orelse return error.PrimaryKeyCannotBeNull;
                defer self.allocator.free(pk_str);

                const k1 = try self.allocator.dupe(u8, pk_str);
                try self.pk_index.put(k1, i);

                const k2 = try self.allocator.dupe(u8, pk_str);
                try self.row_hlcs.put(k2, preserved_hlcs[i]);
            }
        } else {
            // Table has no PK: index rows with synthetic keys in row_hlcs
            for (self.rows.items, 0..) |_, i| {
                var buf: [32]u8 = undefined;
                const synth_key = try std.fmt.bufPrint(&buf, "__row_{d}", .{i + 1});
                const k = try self.allocator.dupe(u8, synth_key);
                try self.row_hlcs.put(k, preserved_hlcs[i]);
            }
        }
    }

    pub fn recreatePrimaryKeyFromMetadata(self: *Table) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.recreatePrimaryKeyFromMetadataInternal();
    }

    pub fn redefinePrimaryKey(self: *Table, new_pk_col: ?[]const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (new_pk_col) |col_name| {
            const new_idx = self.schema.getColumnIndex(col_name) orelse return error.ColumnNotFound;

            // Validate non-null and uniqueness before altering metadata
            var seen = std.StringHashMap(void).init(self.allocator);
            defer {
                var it = seen.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                seen.deinit();
            }

            for (self.rows.items) |row| {
                if (new_idx >= row.values.len or row.values[new_idx] == .null_val) {
                    return error.PrimaryKeyCannotBeNull;
                }
                const s = try self.getPrimaryKeyString(row.values[new_idx]);
                if (seen.contains(s)) {
                    self.allocator.free(s);
                    return error.DuplicatePrimaryKey;
                }
                try seen.put(s, {});
            }

            // Update schema metadata
            for (self.schema.columns, 0..) |*c, idx| {
                if (idx == new_idx) {
                    c.primary_key = true;
                    c.nullable = false;
                } else {
                    c.primary_key = false;
                }
            }
        } else {
            for (self.schema.columns) |*c| {
                c.primary_key = false;
            }
        }

        // Drop the entire primary key and recreate it again from metadata
        try self.recreatePrimaryKeyFromMetadataInternal();
    }

    pub fn dropPrimaryKey(self: *Table) !void {
        return self.redefinePrimaryKey(null);
    }

    pub fn addColumn(self: *Table, col: Column) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.schema.getColumnIndex(col.name) != null) {
            return error.ColumnAlreadyExists;
        }
        if (col.primary_key and self.rows.items.len > 0) {
            return error.PrimaryKeyCannotBeNull;
        }

        if (col.primary_key) {
            for (self.schema.columns) |*c| {
                c.primary_key = false;
            }
        }

        const new_cols = try self.allocator.alloc(Column, self.schema.columns.len + 1);
        errdefer self.allocator.free(new_cols);

        for (self.schema.columns, 0..) |old_col, i| {
            new_cols[i] = try old_col.clone(self.allocator);
        }
        new_cols[self.schema.columns.len] = try col.clone(self.allocator);
        if (col.primary_key) {
            new_cols[self.schema.columns.len].nullable = false;
        }

        for (self.rows.items) |*row| {
            var new_vals = try self.allocator.alloc(Value, row.values.len + 1);
            @memcpy(new_vals[0..row.values.len], row.values);
            new_vals[row.values.len] = Value{ .null_val = {} };
            self.allocator.free(row.values);
            row.values = new_vals;
        }

        self.schema.deinit(self.allocator);
        self.schema.columns = new_cols;

        if (col.primary_key) {
            try self.recreatePrimaryKeyFromMetadataInternal();
        }
    }

    pub fn dropColumn(self: *Table, col_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const drop_idx = self.schema.getColumnIndex(col_name) orelse return error.ColumnNotFound;
        if (self.schema.columns.len <= 1) {
            return error.CannotDropOnlyColumn;
        }

        const was_pk = self.schema.columns[drop_idx].primary_key;

        const new_cols = try self.allocator.alloc(Column, self.schema.columns.len - 1);
        errdefer self.allocator.free(new_cols);

        var dst: usize = 0;
        for (self.schema.columns, 0..) |old_col, src| {
            if (src == drop_idx) continue;
            new_cols[dst] = try old_col.clone(self.allocator);
            dst += 1;
        }

        for (self.rows.items) |*row| {
            row.values[drop_idx].deinit(self.allocator);
            var new_vals = try self.allocator.alloc(Value, row.values.len - 1);
            var v_dst: usize = 0;
            for (row.values, 0..) |val, v_src| {
                if (v_src == drop_idx) continue;
                new_vals[v_dst] = val;
                v_dst += 1;
            }
            self.allocator.free(row.values);
            row.values = new_vals;
        }

        self.schema.deinit(self.allocator);
        self.schema.columns = new_cols;

        if (was_pk) {
            try self.recreatePrimaryKeyFromMetadataInternal();
        }
    }

    pub fn modifyColumn(self: *Table, col: Column) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const mod_idx = self.schema.getColumnIndex(col.name) orelse return error.ColumnNotFound;
        const old_col = &self.schema.columns[mod_idx];

        const is_pk_field = old_col.primary_key or col.primary_key;

        if (col.primary_key and !old_col.primary_key) {
            var seen = std.StringHashMap(void).init(self.allocator);
            defer {
                var it = seen.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                seen.deinit();
            }

            for (self.rows.items) |row| {
                if (mod_idx >= row.values.len or row.values[mod_idx] == .null_val) {
                    return error.PrimaryKeyCannotBeNull;
                }
                const s = try self.getPrimaryKeyString(row.values[mod_idx]);
                if (seen.contains(s)) {
                    self.allocator.free(s);
                    return error.DuplicatePrimaryKey;
                }
                try seen.put(s, {});
            }

            for (self.schema.columns, 0..) |*c, idx| {
                if (idx != mod_idx) {
                    c.primary_key = false;
                }
            }
        }

        // Type conversions on existing data
        if (old_col.data_type != col.data_type) {
            for (self.rows.items) |*row| {
                const val = &row.values[mod_idx];
                switch (col.data_type) {
                    .text => {
                        const str = try val.toStringAlloc(self.allocator);
                        val.deinit(self.allocator);
                        val.* = Value{ .text = str };
                    },
                    .real => {
                        if (val.* == .integer) {
                            val.* = Value{ .real = @floatFromInt(val.integer) };
                        }
                    },
                    .integer => {
                        if (val.* == .real) {
                            val.* = Value{ .integer = @intFromFloat(val.real) };
                        }
                    },
                    else => {},
                }
            }
        }

        old_col.deinit(self.allocator);
        old_col.* = try col.clone(self.allocator);
        if (col.primary_key) {
            old_col.nullable = false;
        }

        if (is_pk_field) {
            try self.recreatePrimaryKeyFromMetadataInternal();
        }
    }

    pub fn renameColumn(self: *Table, old_name: []const u8, new_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const ren_idx = self.schema.getColumnIndex(old_name) orelse return error.ColumnNotFound;
        if (self.schema.getColumnIndex(new_name) != null) {
            return error.ColumnAlreadyExists;
        }

        const is_pk = self.schema.columns[ren_idx].primary_key;

        const duped_new = try self.allocator.dupe(u8, new_name);
        self.allocator.free(self.schema.columns[ren_idx].name);
        self.schema.columns[ren_idx].name = duped_new;

        if (is_pk) {
            try self.recreatePrimaryKeyFromMetadataInternal();
        }
    }

    pub fn getRowState(self: *Table, pk: Value) !?RowState {
        const pk_str = try self.getPrimaryKeyString(pk);
        defer self.allocator.free(pk_str);

        if (self.pk_index.get(pk_str)) |idx| {
            const hlc = self.row_hlcs.get(pk_str) orelse HLC.zero;
            return RowState{
                .hlc = hlc,
                .is_deleted = false,
                .row = self.rows.items[idx],
            };
        }

        if (self.tombstones.get(pk_str)) |tomb_hlc| {
            return RowState{
                .hlc = tomb_hlc,
                .is_deleted = true,
                .row = null,
            };
        }

        return null;
    }

    /// Applies a remote ChangeRecord using deterministic CRDT / HLC resolution.
    pub fn applyRemoteChange(self: *Table, change: ChangeRecord) !ConflictResolution {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pk_str = try self.getPrimaryKeyString(change.primary_key);
        defer self.allocator.free(pk_str);

        var current_state: ?RowState = null;
        if (self.pk_index.get(pk_str)) |idx| {
            const hlc = self.row_hlcs.get(pk_str) orelse HLC.zero;
            current_state = RowState{
                .hlc = hlc,
                .is_deleted = false,
                .row = self.rows.items[idx],
            };
        } else if (self.tombstones.get(pk_str)) |tomb_hlc| {
            current_state = RowState{
                .hlc = tomb_hlc,
                .is_deleted = true,
                .row = null,
            };
        }

        const decision = crdt.resolveChange(current_state, change);
        if (decision != .applied_incoming) {
            return decision;
        }

        // Apply incoming mutation
        switch (change.op) {
            .insert, .update => {
                if (change.row) |incoming_row| {
                    if (self.tombstones.fetchRemove(pk_str)) |entry| {
                        self.allocator.free(entry.key);
                    }

                    if (self.pk_index.get(pk_str)) |idx| {
                        self.rows.items[idx].deinit(self.allocator);
                        self.rows.items[idx] = try incoming_row.clone(self.allocator);
                        if (self.row_hlcs.getPtr(pk_str)) |ptr| {
                            ptr.* = change.hlc;
                        }
                    } else {
                        const new_idx = self.rows.items.len;
                        try self.rows.append(try incoming_row.clone(self.allocator));
                        try self.pk_index.put(try self.allocator.dupe(u8, pk_str), new_idx);
                        try self.row_hlcs.put(try self.allocator.dupe(u8, pk_str), change.hlc);
                    }
                }
            },
            .delete => {
                if (self.pk_index.get(pk_str)) |idx| {
                    self.rows.items[idx].deinit(self.allocator);
                    const last_idx = self.rows.items.len - 1;
                    if (idx != last_idx) {
                        const swapped_row = self.rows.items[last_idx];
                        self.rows.items[idx] = swapped_row;
                        if (try self.getRowPrimaryKeyString(swapped_row)) |swapped_pk_str| {
                            defer self.allocator.free(swapped_pk_str);
                            if (self.pk_index.getPtr(swapped_pk_str)) |ptr| {
                                ptr.* = idx;
                            }
                        }
                    }
                    _ = self.rows.pop();

                    if (self.pk_index.fetchRemove(pk_str)) |entry| {
                        self.allocator.free(entry.key);
                    }
                    if (self.row_hlcs.fetchRemove(pk_str)) |entry| {
                        self.allocator.free(entry.key);
                    }
                }

                if (self.tombstones.getPtr(pk_str)) |tomb_ptr| {
                    tomb_ptr.* = change.hlc;
                } else {
                    try self.tombstones.put(try self.allocator.dupe(u8, pk_str), change.hlc);
                }
            },
        }

        return .applied_incoming;
    }

    /// Serializes entire table to disk format (.sdb)
    pub fn saveToFile(self: *Table, file_path: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const file = try std.fs.cwd().createFile(file_path, .{ .truncate = true });
        defer file.close();
        var buf_writer = std.io.bufferedWriter(file.writer());
        const writer = buf_writer.writer();

        // Magic "SDBT"
        try writer.writeAll("SDBT");
        try writer.writeInt(u16, 2, .little); // version 2

        // Table name & schema
        try writer.writeInt(u16, @intCast(self.name.len), .little);
        try writer.writeAll(self.name);
        try self.schema.serialize(writer);

        // Next auto-increment ID
        try writer.writeInt(i64, self.next_auto_inc, .little);

        // Rows count
        try writer.writeInt(u32, @intCast(self.rows.items.len), .little);
        for (self.rows.items, 0..) |row, r_i| {
            try row.serialize(writer);
            const hlc = if (try self.getRowPrimaryKeyString(row)) |pk_str| blk: {
                defer self.allocator.free(pk_str);
                break :blk self.row_hlcs.get(pk_str) orelse HLC.zero;
            } else blk: {
                var buf: [32]u8 = undefined;
                const synth_key = std.fmt.bufPrint(&buf, "__row_{d}", .{r_i + 1}) catch "";
                break :blk self.row_hlcs.get(synth_key) orelse HLC.zero;
            };
            try hlc.serialize(writer);
        }

        // Tombstones count
        try writer.writeInt(u32, @intCast(self.tombstones.count()), .little);
        var tomb_it = self.tombstones.iterator();
        while (tomb_it.next()) |entry| {
            try writer.writeInt(u16, @intCast(entry.key_ptr.*.len), .little);
            try writer.writeAll(entry.key_ptr.*);
            try entry.value_ptr.*.serialize(writer);
        }

        try buf_writer.flush();
    }

    /// Deserializes table from disk
    pub fn loadFromFile(allocator: std.mem.Allocator, file_path: []const u8) !*Table {
        const file = try std.fs.cwd().openFile(file_path, .{});
        defer file.close();
        var buf_reader = std.io.bufferedReader(file.reader());
        const reader = buf_reader.reader();

        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!std.mem.eql(u8, &magic, "SDBT")) return error.InvalidFileHeader;

        const version = try reader.readInt(u16, .little);
        if (version != 1 and version != 2) return error.UnsupportedVersion;

        const name_len = try reader.readInt(u16, .little);
        const name_buf = try allocator.alloc(u8, name_len);
        defer allocator.free(name_buf);
        try reader.readNoEof(name_buf);

        const schema = try Schema.deserializeVersion(allocator, reader, version);
        defer schema.deinit(allocator);

        const table = try Table.init(allocator, name_buf, schema);
        errdefer table.deinit();

        if (version >= 2) {
            table.next_auto_inc = try reader.readInt(i64, .little);
        }

        const row_count = try reader.readInt(u32, .little);
        try table.rows.ensureTotalCapacity(row_count);
        try table.pk_index.ensureTotalCapacity(@intCast(row_count));
        try table.row_hlcs.ensureTotalCapacity(@intCast(row_count));

        var i: usize = 0;
        while (i < row_count) : (i += 1) {
            const row = try Row.deserialize(allocator, reader);
            const hlc = try HLC.deserialize(reader);
            errdefer row.deinit(allocator);
            try table.insertRowDirect(row, hlc);
        }

        const tomb_count = try reader.readInt(u32, .little);
        var j: usize = 0;
        while (j < tomb_count) : (j += 1) {
            const tomb_len = try reader.readInt(u16, .little);
            const tomb_pk = try allocator.alloc(u8, tomb_len);
            defer allocator.free(tomb_pk);
            try reader.readNoEof(tomb_pk);
            const tomb_hlc = try HLC.deserialize(reader);
            try table.tombstones.put(try allocator.dupe(u8, tomb_pk), tomb_hlc);
        }

        return table;
    }
};

test "primary key define, redefine, drop, and validation" {
    const allocator = std.testing.allocator;
    var hlc_gen = HLC.init([_]u8{1} ** 16);

    const cols = try allocator.alloc(Column, 2);
    cols[0] = Column{ .name = try allocator.dupe(u8, "id"), .data_type = .integer, .primary_key = false, .nullable = true };
    cols[1] = Column{ .name = try allocator.dupe(u8, "email"), .data_type = .text, .primary_key = false, .nullable = true };
    const schema = Schema{ .columns = cols };
    defer schema.deinit(allocator);

    const table = try Table.init(allocator, "users", schema);
    defer table.deinit();

    const r1_vals = try allocator.alloc(Value, 2);
    r1_vals[0] = Value{ .integer = 1 };
    r1_vals[1] = Value{ .text = try allocator.dupe(u8, "alice@example.com") };
    const r1 = Row{ .values = r1_vals };
    defer r1.deinit(allocator);
    try table.insertRow(r1, hlc_gen.now());

    const r2_vals = try allocator.alloc(Value, 2);
    r2_vals[0] = Value{ .integer = 2 };
    r2_vals[1] = Value{ .text = try allocator.dupe(u8, "bob@example.com") };
    const r2 = Row{ .values = r2_vals };
    defer r2.deinit(allocator);
    try table.insertRow(r2, hlc_gen.now());

    try std.testing.expectEqual(@as(?usize, null), table.schema.getPrimaryKeyIndex());

    // Define primary key as "id"
    try table.redefinePrimaryKey("id");
    try std.testing.expectEqual(@as(?usize, 0), table.schema.getPrimaryKeyIndex());
    try std.testing.expectEqual(true, table.schema.columns[0].primary_key);
    try std.testing.expectEqual(false, table.schema.columns[0].nullable);
    try std.testing.expect(table.pk_index.contains("1"));
    try std.testing.expect(table.pk_index.contains("2"));

    // Redefine primary key to "email"
    try table.redefinePrimaryKey("email");
    try std.testing.expectEqual(@as(?usize, 1), table.schema.getPrimaryKeyIndex());
    try std.testing.expectEqual(false, table.schema.columns[0].primary_key);
    try std.testing.expectEqual(true, table.schema.columns[1].primary_key);
    try std.testing.expect(!table.pk_index.contains("1"));
    try std.testing.expect(table.pk_index.contains("alice@example.com"));
    try std.testing.expect(table.pk_index.contains("bob@example.com"));

    // Drop primary key
    try table.dropPrimaryKey();
    try std.testing.expectEqual(@as(?usize, null), table.schema.getPrimaryKeyIndex());
    try std.testing.expectEqual(false, table.schema.columns[1].primary_key);

    // Test validation: duplicate values reject PK definition
    const r3_vals = try allocator.alloc(Value, 2);
    r3_vals[0] = Value{ .integer = 1 };
    r3_vals[1] = Value{ .text = try allocator.dupe(u8, "charlie@example.com") };
    const r3 = Row{ .values = r3_vals };
    defer r3.deinit(allocator);
    try table.insertRow(r3, hlc_gen.now());

    const dup_err = table.redefinePrimaryKey("id");
    try std.testing.expectError(error.DuplicatePrimaryKey, dup_err);

    // Test validation: null value rejects PK definition
    const r4_vals = try allocator.alloc(Value, 2);
    r4_vals[0] = Value{ .null_val = {} };
    r4_vals[1] = Value{ .null_val = {} };
    const r4 = Row{ .values = r4_vals };
    defer r4.deinit(allocator);
    try table.insertRow(r4, hlc_gen.now());

    const null_err = table.redefinePrimaryKey("email");
    try std.testing.expectError(error.PrimaryKeyCannotBeNull, null_err);
}

test "composite primary key multi-column define, validation, and operations" {
    const allocator = std.testing.allocator;
    var hlc_gen = HLC.init([_]u8{1} ** 16);

    const cols = try allocator.alloc(Column, 3);
    cols[0] = Column{ .name = try allocator.dupe(u8, "tenant_id"), .data_type = .integer, .primary_key = true, .nullable = false };
    cols[1] = Column{ .name = try allocator.dupe(u8, "user_id"), .data_type = .integer, .primary_key = true, .nullable = false };
    cols[2] = Column{ .name = try allocator.dupe(u8, "role"), .data_type = .text, .primary_key = false, .nullable = true };
    const schema = Schema{ .columns = cols };
    defer schema.deinit(allocator);

    const table = try Table.init(allocator, "tenant_users", schema);
    defer table.deinit();

    try std.testing.expectEqual(@as(usize, 2), table.schema.getPrimaryKeyCount());
    try std.testing.expect(table.schema.hasPrimaryKey());

    // Insert (tenant=1, user=10, role="admin")
    const r1_vals = try allocator.alloc(Value, 3);
    r1_vals[0] = Value{ .integer = 1 };
    r1_vals[1] = Value{ .integer = 10 };
    r1_vals[2] = Value{ .text = try allocator.dupe(u8, "admin") };
    const r1 = Row{ .values = r1_vals };
    defer r1.deinit(allocator);
    try table.insertRow(r1, hlc_gen.now());

    // Insert (tenant=1, user=20, role="member") -> same tenant_id, different user_id -> valid!
    const r2_vals = try allocator.alloc(Value, 3);
    r2_vals[0] = Value{ .integer = 1 };
    r2_vals[1] = Value{ .integer = 20 };
    r2_vals[2] = Value{ .text = try allocator.dupe(u8, "member") };
    const r2 = Row{ .values = r2_vals };
    defer r2.deinit(allocator);
    try table.insertRow(r2, hlc_gen.now());

    // Insert (tenant=2, user=10, role="guest") -> different tenant_id, same user_id -> valid!
    const r3_vals = try allocator.alloc(Value, 3);
    r3_vals[0] = Value{ .integer = 2 };
    r3_vals[1] = Value{ .integer = 10 };
    r3_vals[2] = Value{ .text = try allocator.dupe(u8, "guest") };
    const r3 = Row{ .values = r3_vals };
    defer r3.deinit(allocator);
    try table.insertRow(r3, hlc_gen.now());

    // Duplicate composite PK: (tenant=1, user=10) -> duplicate!
    const r4_vals = try allocator.alloc(Value, 3);
    r4_vals[0] = Value{ .integer = 1 };
    r4_vals[1] = Value{ .integer = 10 };
    r4_vals[2] = Value{ .text = try allocator.dupe(u8, "duplicate") };
    const r4 = Row{ .values = r4_vals };
    defer r4.deinit(allocator);
    try std.testing.expectError(error.DuplicatePrimaryKey, table.insertRow(r4, hlc_gen.now()));

    // Null in any composite PK column -> rejected!
    const r5_vals = try allocator.alloc(Value, 3);
    r5_vals[0] = Value{ .integer = 3 };
    r5_vals[1] = Value{ .null_val = {} };
    r5_vals[2] = Value{ .text = try allocator.dupe(u8, "bad") };
    const r5 = Row{ .values = r5_vals };
    defer r5.deinit(allocator);
    try std.testing.expectError(error.PrimaryKeyCannotBeNull, table.insertRow(r5, hlc_gen.now()));
}

