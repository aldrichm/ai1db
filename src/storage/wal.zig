const std = @import("std");
const types = @import("../types.zig");
const HLC = @import("../sync/hlc.zig").HLC;
const cdc = @import("../sync/cdc.zig");
const ChangeRecord = cdc.ChangeRecord;

pub const WAL = struct {
    allocator: std.mem.Allocator,
    file_path: []const u8,
    file: ?std.fs.File,
    current_lsn: u64,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) !*WAL {
        const wal = try allocator.create(WAL);
        errdefer allocator.destroy(wal);

        const duped_path = try allocator.dupe(u8, file_path);
        errdefer allocator.free(duped_path);

        const file = try std.fs.cwd().createFile(duped_path, .{
            .read = true,
            .truncate = false,
        });

        // Seek to end
        const end_pos = try file.getEndPos();
        try file.seekTo(end_pos);

        wal.* = WAL{
            .allocator = allocator,
            .file_path = duped_path,
            .file = file,
            .current_lsn = 0,
            .mutex = std.Thread.Mutex{},
        };

        if (end_pos == 0) {
            try wal.writeHeader();
        }

        return wal;
    }

    pub fn deinit(self: *WAL) void {
        if (self.file) |*f| {
            f.close();
            self.file = null;
        }
        self.allocator.free(self.file_path);
        self.allocator.destroy(self);
    }

    fn writeHeader(self: *WAL) !void {
        if (self.file) |file| {
            const writer = file.writer();
            try writer.writeAll("SWAL");
            try writer.writeInt(u16, 1, .little); // version
        }
    }

    pub fn append(self: *WAL, record: ChangeRecord) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const file = self.file orelse return error.FileClosed;
        const writer = file.writer();

        self.current_lsn += 1;
        try writer.writeInt(u64, self.current_lsn, .little);
        try record.serialize(writer);

        return self.current_lsn;
    }

    pub fn flush(self: *WAL) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.file) |file| {
            try file.sync();
        }
    }

    pub fn truncate(self: *WAL) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.file) |*f| {
            f.close();
        }

        const file = try std.fs.cwd().createFile(self.file_path, .{
            .read = true,
            .truncate = true,
        });
        self.file = file;
        self.current_lsn = 0;
        try self.writeHeader();
    }
};
