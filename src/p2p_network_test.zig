const std = @import("std");
const types = @import("types.zig");
const Engine = @import("storage/engine.zig").Engine;
const SyncManager = @import("sync/node.zig").SyncManager;

test "P2P Multi-Node Network Replication and Persistence Test" {
    const allocator = std.testing.allocator;

    const dir_a = "./test_p2p_a";
    const dir_b = "./test_p2p_b";
    std.fs.cwd().deleteTree(dir_a) catch {};
    std.fs.cwd().deleteTree(dir_b) catch {};
    defer {
        std.fs.cwd().deleteTree(dir_a) catch {};
        std.fs.cwd().deleteTree(dir_b) catch {};
    }

    const engine_a = try Engine.init(allocator, dir_a, "Machine-A");
    defer engine_a.deinit();

    const engine_b = try Engine.init(allocator, dir_b, "Machine-B");
    defer engine_b.deinit();

    const sync_a = try SyncManager.init(allocator, engine_a, 19433);
    defer sync_a.deinit();
    try sync_a.start();

    const sync_b = try SyncManager.init(allocator, engine_b, 19434);
    defer sync_b.deinit();
    try sync_b.start();

    // Give server worker time to bind
    std.time.sleep(50 * std.time.ns_per_ms);

    // Connect A to B
    try sync_a.connectToPeer("127.0.0.1", 19434);

    // Give handshake a few ms
    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify peers connected
    const peers_a = try sync_a.getPeerList(allocator);
    defer {
        for (peers_a) |p| {
            allocator.free(p.desktop_name);
            allocator.free(p.address);
        }
        allocator.free(peers_a);
    }
    try std.testing.expectEqual(@as(usize, 1), peers_a.len);

    const peers_b = try sync_b.getPeerList(allocator);
    defer {
        for (peers_b) |p| {
            allocator.free(p.desktop_name);
            allocator.free(p.address);
        }
        allocator.free(peers_b);
    }
    try std.testing.expectEqual(@as(usize, 1), peers_b.len);

    // 1. Create table on Machine A
    const cols = [_]types.Column{
        .{ .name = "id", .data_type = .integer, .primary_key = true, .nullable = false },
        .{ .name = "name", .data_type = .text, .primary_key = false, .nullable = true },
    };
    const schema = try types.Schema.init(allocator, &cols);
    defer schema.deinit(allocator);

    _ = try engine_a.createTable("users", schema);

    // Wait for sync propagation
    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify table exists on Machine B
    {
        engine_b.mutex.lock();
        const table_b = engine_b.tables.get("users");
        engine_b.mutex.unlock();
        try std.testing.expect(table_b != null);
    }

    // 2. Insert row on Machine A
    var row1 = try types.Row.init(allocator, 2);
    defer row1.deinit(allocator);
    row1.values[0] = types.Value{ .integer = 1 };
    row1.values[1] = types.Value{ .text = try allocator.dupe(u8, "Alice") };
    try engine_a.insertRow("users", row1);

    // Wait for mutation propagation
    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify row exists on Machine B
    {
        engine_b.mutex.lock();
        const table_b = engine_b.tables.get("users").?;
        engine_b.mutex.unlock();

        table_b.mutex.lock();
        try std.testing.expectEqual(@as(usize, 1), table_b.rows.items.len);
        try std.testing.expectEqualStrings("Alice", table_b.rows.items[0].values[1].text);
        table_b.mutex.unlock();
    }

    // 3. Update row on Machine A
    var updated_row = try types.Row.init(allocator, 2);
    defer updated_row.deinit(allocator);
    updated_row.values[0] = types.Value{ .integer = 1 };
    updated_row.values[1] = types.Value{ .text = try allocator.dupe(u8, "Alice-Updated") };
    try engine_a.updateRow("users", types.Value{ .integer = 1 }, updated_row);

    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify update on Machine B
    {
        engine_b.mutex.lock();
        const table_b = engine_b.tables.get("users").?;
        engine_b.mutex.unlock();

        table_b.mutex.lock();
        try std.testing.expectEqual(@as(usize, 1), table_b.rows.items.len);
        try std.testing.expectEqualStrings("Alice-Updated", table_b.rows.items[0].values[1].text);
        table_b.mutex.unlock();
    }

    // 4. Reverse sync: Insert row on Machine B
    var row2 = try types.Row.init(allocator, 2);
    defer row2.deinit(allocator);
    row2.values[0] = types.Value{ .integer = 2 };
    row2.values[1] = types.Value{ .text = try allocator.dupe(u8, "Bob") };
    try engine_b.insertRow("users", row2);

    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify row2 reached Machine A
    {
        engine_a.mutex.lock();
        const table_a = engine_a.tables.get("users").?;
        engine_a.mutex.unlock();

        table_a.mutex.lock();
        try std.testing.expectEqual(@as(usize, 2), table_a.rows.items.len);
        table_a.mutex.unlock();
    }

    // 5. Delete row on Machine A
    _ = try engine_a.deleteRow("users", types.Value{ .integer = 1 });

    std.time.sleep(150 * std.time.ns_per_ms);

    // Verify deletion on Machine B
    {
        engine_b.mutex.lock();
        const table_b = engine_b.tables.get("users").?;
        engine_b.mutex.unlock();

        table_b.mutex.lock();
        try std.testing.expectEqual(@as(usize, 1), table_b.rows.items.len);
        try std.testing.expectEqual(@as(i64, 2), table_b.rows.items[0].values[0].integer);
        table_b.mutex.unlock();
    }

    // 6. Verify Machine B Disk Persistence:
    // Ensure the .sdb file on Machine B exists and contains the synchronized data when re-opened
    {
        const engine_b_disk_check = try Engine.init(allocator, dir_b, "Machine-B-Check");
        defer engine_b_disk_check.deinit();

        engine_b_disk_check.mutex.lock();
        const disk_table = engine_b_disk_check.tables.get("users");
        engine_b_disk_check.mutex.unlock();

        try std.testing.expect(disk_table != null);
        disk_table.?.mutex.lock();
        try std.testing.expectEqual(@as(usize, 1), disk_table.?.rows.items.len);
        try std.testing.expectEqual(@as(i64, 2), disk_table.?.rows.items[0].values[0].integer);
        disk_table.?.mutex.unlock();
    }
}
