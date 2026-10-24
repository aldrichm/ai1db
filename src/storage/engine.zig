const std = @import("std");
const types = @import("../types.zig");
const Schema = types.Schema;
const Row = types.Row;
const Value = types.Value;
const Table = @import("table.zig").Table;
const WAL = @import("wal.zig").WAL;
const HLC = @import("../sync/hlc.zig").HLC;
const cdc = @import("../sync/cdc.zig");
const ChangeRecord = cdc.ChangeRecord;
const ChangeLog = cdc.ChangeLog;
const crdt = @import("../sync/crdt.zig");
const ConflictResolution = crdt.ConflictResolution;
const MetadataManager = @import("metadata.zig").MetadataManager;
const protocol = @import("../sync/protocol.zig");
const TableSnapshot = protocol.TableSnapshot;
const ColumnDesc = protocol.ColumnDesc;
const RowSnapshot = protocol.RowSnapshot;
const TombstoneSnapshot = protocol.TombstoneSnapshot;

pub const OnMutationCallback = *const fn (ctx: *anyopaque, record: ChangeRecord) void;
pub const OnDdlCallback = *const fn (ctx: *anyopaque, table_name: []const u8, is_drop: bool) void;

pub const Engine = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    node_id: [16]u8,
    desktop_name: []const u8,
    tables: std.StringHashMap(*Table),
    hlc: HLC,
    cdc_log: *ChangeLog,
    wal: ?*WAL,
    metadata: *MetadataManager,
    mutex: std.Thread.Mutex,

    // Mutation and DDL listeners for real-time peer streaming
    listener_ctx: ?*anyopaque = null,
    on_mutation: ?OnMutationCallback = null,
    on_ddl: ?OnDdlCallback = null,

    pub fn init(
        allocator: std.mem.Allocator,
        data_dir: []const u8,
        desktop_name: []const u8,
    ) !*Engine {
        const engine = try allocator.create(Engine);
        errdefer allocator.destroy(engine);

        // Ensure directory exists
        try std.fs.cwd().makePath(data_dir);

        // Load or create Node ID
        const node_id = try loadOrGenerateNodeId(data_dir);

        const duped_dir = try allocator.dupe(u8, data_dir);
        errdefer allocator.free(duped_dir);

        const duped_name = try allocator.dupe(u8, desktop_name);
        errdefer allocator.free(duped_name);

        const cdc_log = try allocator.create(ChangeLog);
        cdc_log.* = ChangeLog.init(allocator);

        // WAL path
        const wal_path = try std.fs.path.join(allocator, &[_][]const u8{ data_dir, "stratadb.wal" });
        defer allocator.free(wal_path);

        const wal = WAL.init(allocator, wal_path) catch null;

        const metadata = try MetadataManager.init(allocator, data_dir);
        errdefer metadata.deinit();

        engine.* = Engine{
            .allocator = allocator,
            .data_dir = duped_dir,
            .node_id = node_id,
            .desktop_name = duped_name,
            .tables = std.StringHashMap(*Table).init(allocator),
            .hlc = HLC.init(node_id),
            .cdc_log = cdc_log,
            .wal = wal,
            .metadata = metadata,
            .mutex = std.Thread.Mutex{},
        };

        // Load any existing tables from data_dir
        try engine.loadExistingTables();

        return engine;
    }

    pub fn deinit(self: *Engine) void {
        // Checkpoint before exit
        self.saveAllTablesInternal() catch {};

        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.tables.deinit();

        self.metadata.deinit();

        if (self.wal) |w| {
            w.deinit();
        }

        self.cdc_log.deinit();
        self.allocator.destroy(self.cdc_log);

        self.allocator.free(self.desktop_name);
        self.allocator.free(self.data_dir);
        self.allocator.destroy(self);
    }

    fn loadOrGenerateNodeId(data_dir: []const u8) ![16]u8 {
        var path_buf: [512]u8 = undefined;
        const id_file_path = try std.fmt.bufPrint(&path_buf, "{s}/node_id.bin", .{data_dir});

        if (std.fs.cwd().openFile(id_file_path, .{})) |file| {
            defer file.close();
            var nid: [16]u8 = undefined;
            const read_bytes = try file.readAll(&nid);
            if (read_bytes == 16) return nid;
        } else |_| {}

        // Generate cryptographically secure random node ID
        var new_id: [16]u8 = undefined;
        std.crypto.random.bytes(&new_id);

        if (std.fs.cwd().createFile(id_file_path, .{ .truncate = true })) |file| {
            defer file.close();
            try file.writeAll(&new_id);
        } else |_| {}

        return new_id;
    }

    fn loadExistingTables(self: *Engine) !void {
        var dir = try std.fs.cwd().openDir(self.data_dir, .{ .iterate = true });
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".sdb")) {
                const full_path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.data_dir, entry.name });
                defer self.allocator.free(full_path);

                if (Table.loadFromFile(self.allocator, full_path)) |table| {
                    const table_name_dup = try self.allocator.dupe(u8, table.name);
                    try self.tables.put(table_name_dup, table);
                } else |_| {}
            }
        }
    }

    pub fn setMutationListener(self: *Engine, ctx: *anyopaque, cb: OnMutationCallback) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.listener_ctx = ctx;
        self.on_mutation = cb;
    }

    pub fn setDdlListener(self: *Engine, ctx: *anyopaque, cb: OnDdlCallback) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.listener_ctx = ctx;
        self.on_ddl = cb;
    }

    pub fn createTable(self: *Engine, name: []const u8, schema: Schema) !*Table {
        self.mutex.lock();

        if (self.tables.contains(name)) {
            self.mutex.unlock();
            return error.TableAlreadyExists;
        }

        const table = try Table.init(self.allocator, name, schema);
        errdefer table.deinit();

        const dup_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(dup_name);

        try self.tables.put(dup_name, table);

        // Save schema to disk
        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, name, false);

        return table;
    }

    pub fn dropTable(self: *Engine, name: []const u8) !bool {
        const removed = try self.dropTableInternal(name);
        if (removed) {
            self.mutex.lock();
            const listener_ctx = self.listener_ctx;
            const on_ddl = self.on_ddl;
            self.mutex.unlock();
            if (on_ddl) |cb| cb(listener_ctx.?, name, true);
        }
        return removed;
    }

    pub fn dropTableInternal(self: *Engine, name: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.fetchRemove(name)) |entry| {
            self.allocator.free(entry.key);
            entry.value.deinit();

            self.metadata.removeTable(name);

            const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, name });
            defer self.allocator.free(file_name);
            std.fs.cwd().deleteFile(file_name) catch {};
            return true;
        }
        return false;
    }

    pub fn getTable(self: *Engine, name: []const u8) ?*Table {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.tables.get(name);
    }

    pub fn alterTableAddColumn(self: *Engine, table_name: []const u8, col: types.Column) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };
        try table.addColumn(col);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn alterTableDropColumn(self: *Engine, table_name: []const u8, col_name: []const u8) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };
        try table.dropColumn(col_name);

        self.metadata.removeColumn(table_name, col_name);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn alterTableModifyColumn(self: *Engine, table_name: []const u8, col: types.Column) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };
        try table.modifyColumn(col);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn alterTableRenameColumn(self: *Engine, table_name: []const u8, old_name: []const u8, new_name: []const u8) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };
        try table.renameColumn(old_name, new_name);

        self.metadata.renameColumn(table_name, old_name, new_name) catch {};

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn alterTableRedefinePrimaryKey(self: *Engine, table_name: []const u8, new_pk_col: ?[]const u8) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };
        try table.redefinePrimaryKey(new_pk_col);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);

        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();

        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn alterTableDropPrimaryKey(self: *Engine, table_name: []const u8) !void {
        return self.alterTableRedefinePrimaryKey(table_name, null);
    }

    pub fn getTableDescription(self: *Engine, table_name: []const u8) []const u8 {
        return self.metadata.getTableDescription(table_name);
    }

    pub fn getColumnDescription(self: *Engine, table_name: []const u8, column_name: []const u8) []const u8 {
        return self.metadata.getColumnDescription(table_name, column_name);
    }

    pub fn setTableDescription(self: *Engine, table_name: []const u8, description: []const u8) !void {
        try self.metadata.setTableDescription(table_name, description);
        self.mutex.lock();
        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();
        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn setColumnDescription(self: *Engine, table_name: []const u8, column_name: []const u8, description: []const u8) !void {
        try self.metadata.setColumnDescription(table_name, column_name, description);
        self.mutex.lock();
        const listener_ctx = self.listener_ctx;
        const on_ddl = self.on_ddl;
        self.mutex.unlock();
        if (on_ddl) |cb| cb(listener_ctx.?, table_name, false);
    }

    pub fn createTableSnapshot(self: *Engine, table_name: []const u8, allocator: std.mem.Allocator) !?TableSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const table = self.tables.get(table_name) orelse return null;
        table.mutex.lock();
        defer table.mutex.unlock();

        const dup_name = try allocator.dupe(u8, table.name);
        errdefer allocator.free(dup_name);

        const cloned_schema = try table.schema.clone(allocator);
        errdefer {
            var s = cloned_schema;
            s.deinit(allocator);
        }

        const tbl_desc = self.metadata.getTableDescription(table.name);
        const dup_desc = try allocator.dupe(u8, tbl_desc);
        errdefer allocator.free(dup_desc);

        var col_descs = std.ArrayList(ColumnDesc).init(allocator);
        errdefer {
            for (col_descs.items) |cd| {
                allocator.free(cd.col_name);
                allocator.free(cd.desc);
            }
            col_descs.deinit();
        }

        for (table.schema.columns) |col| {
            const cd_val = self.metadata.getColumnDescription(table.name, col.name);
            if (cd_val.len > 0) {
                try col_descs.append(ColumnDesc{
                    .col_name = try allocator.dupe(u8, col.name),
                    .desc = try allocator.dupe(u8, cd_val),
                });
            }
        }

        var rows = try allocator.alloc(RowSnapshot, table.rows.items.len);
        errdefer allocator.free(rows);
        var r_i: usize = 0;
        errdefer {
            for (rows[0..r_i]) |r| {
                var rw = r.row;
                rw.deinit(allocator);
            }
        }
        for (table.rows.items, 0..) |row, i| {
            const cloned_row = try row.clone(allocator);
            const hlc = if (try table.getRowPrimaryKeyString(row)) |pk_str| blk: {
                defer table.allocator.free(pk_str);
                break :blk table.row_hlcs.get(pk_str) orelse HLC.zero;
            } else blk: {
                var buf: [32]u8 = undefined;
                const synth_key = std.fmt.bufPrint(&buf, "__row_{d}", .{i + 1}) catch "";
                break :blk table.row_hlcs.get(synth_key) orelse HLC.zero;
            };
            rows[i] = RowSnapshot{
                .row = cloned_row,
                .hlc = hlc,
            };
            r_i += 1;
        }

        var tombstones = try allocator.alloc(TombstoneSnapshot, table.tombstones.count());
        errdefer allocator.free(tombstones);
        var t_i: usize = 0;
        errdefer {
            for (tombstones[0..t_i]) |t| allocator.free(t.pk_str);
        }
        var tomb_it = table.tombstones.iterator();
        while (tomb_it.next()) |entry| {
            tombstones[t_i] = TombstoneSnapshot{
                .pk_str = try allocator.dupe(u8, entry.key_ptr.*),
                .hlc = entry.value_ptr.*,
            };
            t_i += 1;
        }

        return TableSnapshot{
            .name = dup_name,
            .schema = cloned_schema,
            .description = dup_desc,
            .column_descriptions = try col_descs.toOwnedSlice(),
            .next_auto_inc = table.next_auto_inc,
            .rows = rows,
            .tombstones = tombstones,
        };
    }

    pub fn applyTableSnapshot(self: *Engine, snapshot: TableSnapshot) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var table_ptr: *Table = undefined;
        if (self.tables.get(snapshot.name)) |t| {
            table_ptr = t;
            // Check if remote has columns that local doesn't have
            for (snapshot.schema.columns) |col| {
                if (table_ptr.schema.getColumnIndex(col.name) == null) {
                    try table_ptr.addColumn(col);
                }
            }
        } else {
            // Table doesn't exist locally - create it!
            const new_table = try Table.init(self.allocator, snapshot.name, snapshot.schema);
            errdefer new_table.deinit();

            const dup_name = try self.allocator.dupe(u8, snapshot.name);
            errdefer self.allocator.free(dup_name);

            try self.tables.put(dup_name, new_table);
            table_ptr = new_table;
        }

        // Apply metadata descriptions
        if (snapshot.description.len > 0) {
            try self.metadata.setTableDescription(snapshot.name, snapshot.description);
        }
        for (snapshot.column_descriptions) |cd| {
            if (cd.desc.len > 0) {
                try self.metadata.setColumnDescription(snapshot.name, cd.col_name, cd.desc);
            }
        }

        table_ptr.mutex.lock();

        // Apply rows using CRDT HLC resolution
        for (snapshot.rows) |r_snap| {
            _ = self.hlc.update(r_snap.hlc);

            const has_pk = table_ptr.schema.hasPrimaryKey();
            if (has_pk) {
                if (try table_ptr.getRowPrimaryKeyString(r_snap.row)) |pk_str| {
                    defer table_ptr.allocator.free(pk_str);

                    if (table_ptr.pk_index.get(pk_str)) |local_idx| {
                        const local_hlc = table_ptr.row_hlcs.get(pk_str) orelse HLC.zero;
                        if (r_snap.hlc.compare(local_hlc) == .gt) {
                            // Incoming row is newer
                            table_ptr.rows.items[local_idx].deinit(table_ptr.allocator);
                            table_ptr.rows.items[local_idx] = try r_snap.row.clone(table_ptr.allocator);
                            if (table_ptr.row_hlcs.getPtr(pk_str)) |ptr| {
                                ptr.* = r_snap.hlc;
                            }
                        }
                    } else if (table_ptr.tombstones.get(pk_str)) |tomb_hlc| {
                        if (r_snap.hlc.compare(tomb_hlc) == .gt) {
                            // Re-inserting previously deleted row because incoming insert is newer
                            if (table_ptr.tombstones.fetchRemove(pk_str)) |entry| {
                                table_ptr.allocator.free(entry.key);
                            }
                            const new_idx = table_ptr.rows.items.len;
                            try table_ptr.rows.append(try r_snap.row.clone(table_ptr.allocator));
                            try table_ptr.pk_index.put(try table_ptr.allocator.dupe(u8, pk_str), new_idx);
                            try table_ptr.row_hlcs.put(try table_ptr.allocator.dupe(u8, pk_str), r_snap.hlc);
                        }
                    } else {
                        // Brand new row
                        const new_idx = table_ptr.rows.items.len;
                        try table_ptr.rows.append(try r_snap.row.clone(table_ptr.allocator));
                        try table_ptr.pk_index.put(try table_ptr.allocator.dupe(u8, pk_str), new_idx);
                        try table_ptr.row_hlcs.put(try table_ptr.allocator.dupe(u8, pk_str), r_snap.hlc);
                    }
                }
            } else {
                // No PK: if not already present, insert
                var found = false;
                for (table_ptr.rows.items) |existing_row| {
                    if (existing_row.isEqual(r_snap.row)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    try table_ptr.rows.append(try r_snap.row.clone(table_ptr.allocator));
                }
            }
        }

        // Apply tombstones
        for (snapshot.tombstones) |t_snap| {
            _ = self.hlc.update(t_snap.hlc);
            if (table_ptr.pk_index.get(t_snap.pk_str)) |local_idx| {
                const local_hlc = table_ptr.row_hlcs.get(t_snap.pk_str) orelse HLC.zero;
                if (t_snap.hlc.compare(local_hlc) == .gt) {
                    // Tombstone is newer than active row - delete row!
                    table_ptr.rows.items[local_idx].deinit(table_ptr.allocator);
                    const last_idx = table_ptr.rows.items.len - 1;
                    if (local_idx != last_idx) {
                        const swapped = table_ptr.rows.items[last_idx];
                        table_ptr.rows.items[local_idx] = swapped;
                        if (try table_ptr.getRowPrimaryKeyString(swapped)) |swapped_pk| {
                            defer table_ptr.allocator.free(swapped_pk);
                            if (table_ptr.pk_index.getPtr(swapped_pk)) |ptr| {
                                ptr.* = local_idx;
                            }
                        }
                    }
                    _ = table_ptr.rows.pop();
                    if (table_ptr.pk_index.fetchRemove(t_snap.pk_str)) |entry| {
                        table_ptr.allocator.free(entry.key);
                    }
                    if (table_ptr.row_hlcs.fetchRemove(t_snap.pk_str)) |entry| {
                        table_ptr.allocator.free(entry.key);
                    }
                    try table_ptr.tombstones.put(try table_ptr.allocator.dupe(u8, t_snap.pk_str), t_snap.hlc);
                }
            } else if (table_ptr.tombstones.get(t_snap.pk_str)) |cur_tomb_hlc| {
                if (t_snap.hlc.compare(cur_tomb_hlc) == .gt) {
                    if (table_ptr.tombstones.getPtr(t_snap.pk_str)) |ptr| {
                        ptr.* = t_snap.hlc;
                    }
                }
            } else {
                try table_ptr.tombstones.put(try table_ptr.allocator.dupe(u8, t_snap.pk_str), t_snap.hlc);
            }
        }

        // Auto-increment
        table_ptr.next_auto_inc = @max(table_ptr.next_auto_inc, snapshot.next_auto_inc);
        table_ptr.mutex.unlock();

        // Persist to disk
        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, snapshot.name });
        defer self.allocator.free(file_name);
        try table_ptr.saveToFile(file_name);

        try self.metadata.saveToFile();
    }

    pub fn listTables(self: *Engine, allocator: std.mem.Allocator) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const list = try allocator.alloc([]const u8, self.tables.count());
        var idx: usize = 0;
        var it = self.tables.keyIterator();
        while (it.next()) |k| {
            list[idx] = try allocator.dupe(u8, k.*);
            idx += 1;
        }
        return list;
    }

    pub fn insertRow(self: *Engine, table_name: []const u8, row: Row) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };

        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        try table.insertRow(row, current_hlc);
        const pk_val = try table.getRowPrimaryKey(row);
        defer if (table.schema.getPrimaryKeyCount() > 1) pk_val.deinit(table.allocator);

        const record = ChangeRecord{
            .table_name = table_name,
            .primary_key = pk_val,
            .op = .insert,
            .hlc = current_hlc,
            .origin_node = self.node_id,
            .row = row,
        };

        if (self.wal) |w| {
            _ = w.append(record) catch {};
        }

        try self.cdc_log.append(record);

        // Notify real-time sync broadcaster
        if (self.on_mutation) |cb| {
            if (self.listener_ctx) |ctx| {
                cb(ctx, record);
            }
        }
    }

    pub fn updateRow(self: *Engine, table_name: []const u8, pk: Value, new_row: Row) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };

        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        try table.updateRow(pk, new_row, current_hlc);

        const record = ChangeRecord{
            .table_name = table_name,
            .primary_key = pk,
            .op = .update,
            .hlc = current_hlc,
            .origin_node = self.node_id,
            .row = new_row,
        };

        if (self.wal) |w| {
            _ = w.append(record) catch {};
        }

        try self.cdc_log.append(record);

        if (self.on_mutation) |cb| {
            if (self.listener_ctx) |ctx| {
                cb(ctx, record);
            }
        }
    }

    pub fn deleteRow(self: *Engine, table_name: []const u8, pk: Value) !bool {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };

        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        const deleted = try table.deleteRow(pk, current_hlc);
        if (!deleted) return false;

        const record = ChangeRecord{
            .table_name = table_name,
            .primary_key = pk,
            .op = .delete,
            .hlc = current_hlc,
            .origin_node = self.node_id,
            .row = null,
        };

        if (self.wal) |w| {
            _ = w.append(record) catch {};
        }

        try self.cdc_log.append(record);

        if (self.on_mutation) |cb| {
            if (self.listener_ctx) |ctx| {
                cb(ctx, record);
            }
        }

        return true;
    }

    pub fn updateRowByIndex(self: *Engine, table_name: []const u8, idx: usize, new_row: Row) !void {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };

        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        try table.updateRowByIndex(idx, new_row, current_hlc);
        const pk_val = try table.getRowPrimaryKey(new_row);
        defer if (table.schema.getPrimaryKeyCount() > 1) pk_val.deinit(table.allocator);

        const record = ChangeRecord{
            .table_name = table_name,
            .primary_key = pk_val,
            .op = .update,
            .hlc = current_hlc,
            .origin_node = self.node_id,
            .row = new_row,
        };

        if (self.wal) |w| {
            _ = w.append(record) catch {};
        }

        try self.cdc_log.append(record);

        if (self.on_mutation) |cb| {
            if (self.listener_ctx) |ctx| {
                cb(ctx, record);
            }
        }
    }

    pub fn deleteRowByIndex(self: *Engine, table_name: []const u8, idx: usize) !bool {
        self.mutex.lock();
        const table = self.tables.get(table_name) orelse {
            self.mutex.unlock();
            return error.TableNotFound;
        };

        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        table.mutex.lock();
        if (idx >= table.rows.items.len) {
            table.mutex.unlock();
            return false;
        }
        const pk_val = try table.getRowPrimaryKey(table.rows.items[idx]);
        table.mutex.unlock();
        defer if (table.schema.getPrimaryKeyCount() > 1) pk_val.deinit(table.allocator);

        const deleted = try table.deleteRowByIndex(idx, current_hlc);
        if (!deleted) return false;

        const record = ChangeRecord{
            .table_name = table_name,
            .primary_key = pk_val,
            .op = .delete,
            .hlc = current_hlc,
            .origin_node = self.node_id,
            .row = null,
        };

        if (self.wal) |w| {
            _ = w.append(record) catch {};
        }

        try self.cdc_log.append(record);

        if (self.on_mutation) |cb| {
            if (self.listener_ctx) |ctx| {
                cb(ctx, record);
            }
        }

        return true;
    }

    /// Applies a remote ChangeRecord received from another desktop.
    pub fn applyRemoteChange(self: *Engine, record: ChangeRecord) !ConflictResolution {
        self.mutex.lock();
        _ = self.hlc.update(record.hlc);
        const table = self.tables.get(record.table_name);
        self.mutex.unlock();

        if (table == null) {
            return .retained_local;
        }

        const resolution = try table.?.applyRemoteChange(record);
        if (resolution == .applied_incoming) {
            // Also append to our local CDC so downstream peers get synced transitively!
            try self.cdc_log.append(record);
        }
        return resolution;
    }

    pub fn checkpoint(self: *Engine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.saveAllTablesInternal();
        if (self.wal) |w| {
            try w.truncate();
        }
    }

    fn saveAllTablesInternal(self: *Engine) !void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table_name = entry.key_ptr.*;
            const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
            defer self.allocator.free(file_name);
            try entry.value_ptr.*.saveToFile(file_name);
        }
    }
};
