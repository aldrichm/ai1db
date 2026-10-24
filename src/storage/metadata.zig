const std = @import("std");

fn writeJsonString(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0...8, 11...12, 14...31 => try std.fmt.format(writer, "\\u{x:0>4}", .{c}),
            else => try writer.writeByte(c),
        }
    }
    try writer.writeByte('"');
}

pub const TableMetadata = struct {
    name: []const u8,
    description: []const u8 = "",
    columns: std.StringHashMap([]const u8), // column_name -> description
};

pub const MetadataManager = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    file_path: []const u8,
    tables: std.StringHashMap(TableMetadata),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*MetadataManager {
        const mgr = try allocator.create(MetadataManager);
        errdefer allocator.destroy(mgr);

        const dup_dir = try allocator.dupe(u8, data_dir);
        errdefer allocator.free(dup_dir);

        const file_path = try std.fs.path.join(allocator, &[_][]const u8{ data_dir, "metadata.json" });
        errdefer allocator.free(file_path);

        mgr.* = MetadataManager{
            .allocator = allocator,
            .data_dir = dup_dir,
            .file_path = file_path,
            .tables = std.StringHashMap(TableMetadata).init(allocator),
            .mutex = std.Thread.Mutex{},
        };

        mgr.loadFromFile() catch {};
        return mgr;
    }

    pub fn deinit(self: *MetadataManager) void {
        self.saveToFile() catch {};

        var t_it = self.tables.iterator();
        while (t_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.description);
            var c_it = entry.value_ptr.columns.iterator();
            while (c_it.next()) |col_entry| {
                self.allocator.free(col_entry.key_ptr.*);
                self.allocator.free(col_entry.value_ptr.*);
            }
            entry.value_ptr.columns.deinit();
        }
        self.tables.deinit();

        self.allocator.free(self.file_path);
        self.allocator.free(self.data_dir);
        self.allocator.destroy(self);
    }

    pub fn getTableDescription(self: *MetadataManager, table_name: []const u8) []const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.get(table_name)) |tbl| {
            return tbl.description;
        }
        return "";
    }

    pub fn getColumnDescription(self: *MetadataManager, table_name: []const u8, column_name: []const u8) []const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.get(table_name)) |tbl| {
            if (tbl.columns.get(column_name)) |desc| {
                return desc;
            }
        }
        return "";
    }

    pub fn setTableDescription(self: *MetadataManager, table_name: []const u8, description: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.getPtr(table_name)) |tbl| {
            self.allocator.free(tbl.description);
            tbl.description = try self.allocator.dupe(u8, description);
        } else {
            const duped_name = try self.allocator.dupe(u8, table_name);
            errdefer self.allocator.free(duped_name);
            const duped_desc = try self.allocator.dupe(u8, description);
            errdefer self.allocator.free(duped_desc);

            try self.tables.put(duped_name, TableMetadata{
                .name = duped_name,
                .description = duped_desc,
                .columns = std.StringHashMap([]const u8).init(self.allocator),
            });
        }
        self.saveToFileInternal() catch {};
    }

    pub fn setColumnDescription(self: *MetadataManager, table_name: []const u8, column_name: []const u8, description: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var tbl_ptr = self.tables.getPtr(table_name);
        if (tbl_ptr == null) {
            const duped_name = try self.allocator.dupe(u8, table_name);
            errdefer self.allocator.free(duped_name);
            const empty_desc = try self.allocator.dupe(u8, "");
            errdefer self.allocator.free(empty_desc);

            try self.tables.put(duped_name, TableMetadata{
                .name = duped_name,
                .description = empty_desc,
                .columns = std.StringHashMap([]const u8).init(self.allocator),
            });
            tbl_ptr = self.tables.getPtr(table_name);
        }

        const tbl = tbl_ptr.?;
        if (tbl.columns.getPtr(column_name)) |desc_ptr| {
            self.allocator.free(desc_ptr.*);
            desc_ptr.* = try self.allocator.dupe(u8, description);
        } else {
            const duped_col_name = try self.allocator.dupe(u8, column_name);
            errdefer self.allocator.free(duped_col_name);
            const duped_desc = try self.allocator.dupe(u8, description);
            errdefer self.allocator.free(duped_desc);

            try tbl.columns.put(duped_col_name, duped_desc);
        }
        self.saveToFileInternal() catch {};
    }

    pub fn renameColumn(self: *MetadataManager, table_name: []const u8, old_name: []const u8, new_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.getPtr(table_name)) |tbl| {
            if (tbl.columns.fetchRemove(old_name)) |entry| {
                defer self.allocator.free(entry.key);
                const duped_new_name = try self.allocator.dupe(u8, new_name);
                try tbl.columns.put(duped_new_name, entry.value);
            }
        }
        self.saveToFileInternal() catch {};
    }

    pub fn removeColumn(self: *MetadataManager, table_name: []const u8, column_name: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.getPtr(table_name)) |tbl| {
            if (tbl.columns.fetchRemove(column_name)) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
        }
        self.saveToFileInternal() catch {};
    }

    pub fn removeTable(self: *MetadataManager, table_name: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.fetchRemove(table_name)) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value.description);
            var col_it = entry.value.columns.iterator();
            while (col_it.next()) |c_entry| {
                self.allocator.free(c_entry.key_ptr.*);
                self.allocator.free(c_entry.value_ptr.*);
            }
            var cols = entry.value.columns;
            cols.deinit();
        }
        self.saveToFileInternal() catch {};
    }

    pub fn saveToFile(self: *MetadataManager) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.saveToFileInternal();
    }

    fn saveToFileInternal(self: *MetadataManager) !void {
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        const writer = buf.writer();

        try writer.writeAll("{\"tables\":[");
        var t_it = self.tables.iterator();
        var t_idx: usize = 0;
        while (t_it.next()) |entry| {
            if (t_idx > 0) try writer.writeAll(",");
            t_idx += 1;

            try writer.writeAll("{\"name\":");
            try writeJsonString(writer, entry.key_ptr.*);
            try writer.writeAll(",\"description\":");
            try writeJsonString(writer, entry.value_ptr.description);
            try writer.writeAll(",\"columns\":[");

            var c_it = entry.value_ptr.columns.iterator();
            var c_idx: usize = 0;
            while (c_it.next()) |col_entry| {
                if (c_idx > 0) try writer.writeAll(",");
                c_idx += 1;
                try writer.writeAll("{\"name\":");
                try writeJsonString(writer, col_entry.key_ptr.*);
                try writer.writeAll(",\"description\":");
                try writeJsonString(writer, col_entry.value_ptr.*);
                try writer.writeAll("}");
            }
            try writer.writeAll("]}");
        }
        try writer.writeAll("]}");

        const file = if (std.fs.path.isAbsolute(self.file_path))
            try std.fs.createFileAbsolute(self.file_path, .{ .truncate = true })
        else
            try std.fs.cwd().createFile(self.file_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(buf.items);
    }

    pub fn loadFromFile(self: *MetadataManager) !void {
        const file = if (std.fs.path.isAbsolute(self.file_path))
            std.fs.openFileAbsolute(self.file_path, .{}) catch return
        else
            std.fs.cwd().openFile(self.file_path, .{}) catch return;
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, 10 * 1024 * 1024);
        defer self.allocator.free(content);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_alloc = arena.allocator();

        const StoredColumn = struct {
            name: []const u8,
            description: []const u8 = "",
        };
        const StoredTable = struct {
            name: []const u8,
            description: []const u8 = "",
            columns: []StoredColumn = &.{},
        };
        const StoredDoc = struct {
            tables: []StoredTable = &.{},
        };

        const parsed = std.json.parseFromSlice(StoredDoc, arena_alloc, content, .{ .ignore_unknown_fields = true }) catch return;
        defer parsed.deinit();

        for (parsed.value.tables) |t| {
            try self.setTableDescription(t.name, t.description);
            for (t.columns) |c| {
                try self.setColumnDescription(t.name, c.name, c.description);
            }
        }
    }
};

test "MetadataManager basic operations" {
    const allocator = std.testing.allocator;
    const test_dir = "test_metadata_dir";
    std.fs.cwd().makePath(test_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var mgr = try MetadataManager.init(allocator, test_dir);
    defer mgr.deinit();

    try mgr.setTableDescription("orders", "Customer purchase orders");
    try mgr.setColumnDescription("orders", "id", "Primary order number");
    try mgr.setColumnDescription("orders", "amount", "Total billing amount in cents");

    try std.testing.expectEqualStrings("Customer purchase orders", mgr.getTableDescription("orders"));
    try std.testing.expectEqualStrings("Primary order number", mgr.getColumnDescription("orders", "id"));
    try std.testing.expectEqualStrings("Total billing amount in cents", mgr.getColumnDescription("orders", "amount"));
    try std.testing.expectEqualStrings("", mgr.getColumnDescription("orders", "nonexistent"));

    // Test persist & reload
    var mgr2 = try MetadataManager.init(allocator, test_dir);
    defer mgr2.deinit();

    try std.testing.expectEqualStrings("Customer purchase orders", mgr2.getTableDescription("orders"));
    try std.testing.expectEqualStrings("Primary order number", mgr2.getColumnDescription("orders", "id"));
}
