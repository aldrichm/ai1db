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

pub const OnMutationCallback = *const fn (ctx: *anyopaque, record: ChangeRecord) void;

pub const Engine = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    node_id: [16]u8,
    desktop_name: []const u8,
    tables: std.StringHashMap(*Table),
    hlc: HLC,
    cdc_log: *ChangeLog,
    wal: ?*WAL,
    mutex: std.Thread.Mutex,

    // Mutation listeners for real-time peer streaming
    listener_ctx: ?*anyopaque = null,
    on_mutation: ?OnMutationCallback = null,

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

        engine.* = Engine{
            .allocator = allocator,
            .data_dir = duped_dir,
            .node_id = node_id,
            .desktop_name = duped_name,
            .tables = std.StringHashMap(*Table).init(allocator),
            .hlc = HLC.init(node_id),
            .cdc_log = cdc_log,
            .wal = wal,
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

    pub fn createTable(self: *Engine, name: []const u8, schema: Schema) !*Table {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.contains(name)) {
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

        return table;
    }

    pub fn dropTable(self: *Engine, name: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.fetchRemove(name)) |entry| {
            self.allocator.free(entry.key);
            entry.value.deinit();

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
        defer self.mutex.unlock();

        const table = self.tables.get(table_name) orelse return error.TableNotFound;
        try table.addColumn(col);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);
    }

    pub fn alterTableDropColumn(self: *Engine, table_name: []const u8, col_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const table = self.tables.get(table_name) orelse return error.TableNotFound;
        try table.dropColumn(col_name);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);
    }

    pub fn alterTableModifyColumn(self: *Engine, table_name: []const u8, col: types.Column) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const table = self.tables.get(table_name) orelse return error.TableNotFound;
        try table.modifyColumn(col);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);
    }

    pub fn alterTableRenameColumn(self: *Engine, table_name: []const u8, old_name: []const u8, new_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const table = self.tables.get(table_name) orelse return error.TableNotFound;
        try table.renameColumn(old_name, new_name);

        const file_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}.sdb", .{ self.data_dir, table_name });
        defer self.allocator.free(file_name);
        try table.saveToFile(file_name);
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

        const pk_val = try table.getRowPrimaryKey(row);
        const current_hlc = self.hlc.now();
        self.mutex.unlock();

        try table.insertRow(row, current_hlc);

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
