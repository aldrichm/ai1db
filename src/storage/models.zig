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

pub const SavedModel = struct {
    name: []const u8,
    json_payload: []const u8,
    updated_at: i64,
};

pub const ModelManager = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    file_path: []const u8,
    models: std.StringHashMap(SavedModel),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*ModelManager {
        const mgr = try allocator.create(ModelManager);
        errdefer allocator.destroy(mgr);

        const dup_dir = try allocator.dupe(u8, data_dir);
        errdefer allocator.free(dup_dir);

        const file_path = try std.fs.path.join(allocator, &[_][]const u8{ data_dir, "models.json" });
        errdefer allocator.free(file_path);

        mgr.* = ModelManager{
            .allocator = allocator,
            .data_dir = dup_dir,
            .file_path = file_path,
            .models = std.StringHashMap(SavedModel).init(allocator),
            .mutex = std.Thread.Mutex{},
        };

        mgr.loadFromFile() catch {};
        return mgr;
    }

    pub fn deinit(self: *ModelManager) void {
        self.saveToFile() catch {};

        var it = self.models.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.name);
            self.allocator.free(entry.value_ptr.json_payload);
        }
        self.models.deinit();

        self.allocator.free(self.file_path);
        self.allocator.free(self.data_dir);
        self.allocator.destroy(self);
    }

    pub fn saveModel(self: *ModelManager, name: []const u8, json_payload: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = std.time.timestamp();

        if (self.models.getPtr(name)) |existing| {
            self.allocator.free(existing.json_payload);
            existing.json_payload = try self.allocator.dupe(u8, json_payload);
            existing.updated_at = now;
        } else {
            const dup_name = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(dup_name);

            const dup_payload = try self.allocator.dupe(u8, json_payload);
            errdefer self.allocator.free(dup_payload);

            const key_dup = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(key_dup);

            try self.models.put(key_dup, SavedModel{
                .name = dup_name,
                .json_payload = dup_payload,
                .updated_at = now,
            });
        }

        self.saveToFileInternal() catch {};
    }

    pub fn deleteModel(self: *ModelManager, name: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.models.fetchRemove(name)) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value.name);
            self.allocator.free(entry.value.json_payload);
            self.saveToFileInternal() catch {};
            return true;
        }
        return false;
    }

    pub fn getModel(self: *ModelManager, name: []const u8, out_allocator: std.mem.Allocator) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.models.get(name)) |m| {
            return out_allocator.dupe(u8, m.json_payload) catch null;
        }
        return null;
    }

    pub fn getAllModelsJson(self: *ModelManager, out_allocator: std.mem.Allocator) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.ArrayList(u8).init(out_allocator);
        errdefer buf.deinit();
        const writer = buf.writer();

        try writer.writeAll("[");
        var it = self.models.iterator();
        var idx: usize = 0;
        while (it.next()) |entry| {
            if (idx > 0) try writer.writeAll(",");
            idx += 1;
            // Write json_payload directly if it's already valid JSON
            try writer.writeAll(entry.value_ptr.json_payload);
        }
        try writer.writeAll("]");

        return try buf.toOwnedSlice();
    }

    pub fn saveToFile(self: *ModelManager) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.saveToFileInternal();
    }

    fn saveToFileInternal(self: *ModelManager) !void {
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();
        const writer = buf.writer();

        try writer.writeAll("{\"models\":[");
        var it = self.models.iterator();
        var idx: usize = 0;
        while (it.next()) |entry| {
            if (idx > 0) try writer.writeAll(",");
            idx += 1;
            try writer.writeAll(entry.value_ptr.json_payload);
        }
        try writer.writeAll("]}");

        const file = try std.fs.cwd().createFile(self.file_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(buf.items);
    }

    fn loadFromFile(self: *ModelManager) !void {
        const file = std.fs.cwd().openFile(self.file_path, .{}) catch return;
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 10 * 1024 * 1024) catch return;
        defer self.allocator.free(content);

        const ParsedContainer = struct {
            models: []std.json.Value,
        };

        var parsed = std.json.parseFromSlice(ParsedContainer, self.allocator, content, .{ .ignore_unknown_fields = true }) catch return;
        defer parsed.deinit();

        for (parsed.value.models) |m_val| {
            if (m_val == .object) {
                if (m_val.object.get("name")) |name_val| {
                    if (name_val == .string and name_val.string.len > 0) {
                        const m_name = name_val.string;

                        // Serialize single model object back to string
                        var s_buf = std.ArrayList(u8).init(self.allocator);
                        defer s_buf.deinit();
                        std.json.stringify(m_val, .{}, s_buf.writer()) catch continue;

                        const dup_name = self.allocator.dupe(u8, m_name) catch continue;
                        const dup_payload = self.allocator.dupe(u8, s_buf.items) catch {
                            self.allocator.free(dup_name);
                            continue;
                        };
                        const key_dup = self.allocator.dupe(u8, m_name) catch {
                            self.allocator.free(dup_name);
                            self.allocator.free(dup_payload);
                            continue;
                        };

                        self.models.put(key_dup, SavedModel{
                            .name = dup_name,
                            .json_payload = dup_payload,
                            .updated_at = std.time.timestamp(),
                        }) catch {
                            self.allocator.free(dup_name);
                            self.allocator.free(dup_payload);
                            self.allocator.free(key_dup);
                        };
                    }
                }
            }
        }
    }
};
