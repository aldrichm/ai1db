const std = @import("std");
const types = @import("types.zig");
const DataType = types.DataType;
const Column = types.Column;
const Schema = types.Schema;
const Row = types.Row;
const Value = types.Value;
const Engine = @import("storage/engine.zig").Engine;
const HLC = @import("sync/hlc.zig").HLC;
const cdc = @import("sync/cdc.zig");
const Parser = @import("sql/parser.zig").Parser;
const Executor = @import("sql/executor.zig").Executor;

fn runSql(allocator: std.mem.Allocator, executor: *Executor, query: []const u8) !@import("sql/executor.zig").QueryResult {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var parser = try Parser.init(arena_alloc, query);
    defer parser.deinit();
    const stmt = try parser.parse();
    return try executor.execute(stmt);
}

test "Basic Relational SQL Operations" {
    const allocator = std.testing.allocator;

    const tmp_dir = "./test_data_sql";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const engine = try Engine.init(allocator, tmp_dir, "TestNode");
    defer engine.deinit();

    var executor = Executor.init(allocator, engine);

    // 1. CREATE TABLE
    {
        var res = try runSql(allocator, &executor, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, salary REAL, active BOOL);");
        defer res.deinit();
        try std.testing.expect(res.columns.len == 0);
    }

    // 2. INSERT
    {
        var r1 = try runSql(allocator, &executor, "INSERT INTO employees VALUES (1, 'Alice', 95000.5, true);");
        defer r1.deinit();

        var r2 = try runSql(allocator, &executor, "INSERT INTO employees VALUES (2, 'Bob', 82000.0, false);");
        defer r2.deinit();

        var r3 = try runSql(allocator, &executor, "INSERT INTO employees VALUES (3, 'Charlie', 110000.0, true);");
        defer r3.deinit();
    }

    // 3. SELECT with WHERE and ORDER BY
    {
        var res = try runSql(allocator, &executor, "SELECT id, name, salary FROM employees WHERE active = true ORDER BY salary DESC;");
        defer res.deinit();

        try std.testing.expectEqual(@as(usize, 2), res.rows.len);
        // First row should be Charlie (110000)
        try std.testing.expectEqualStrings("Charlie", res.rows[0][1].text);
        // Second row should be Alice (95000.5)
        try std.testing.expectEqualStrings("Alice", res.rows[1][1].text);
    }

    // 4. AGGREGATES: COUNT, AVG, MAX
    {
        var res = try runSql(allocator, &executor, "SELECT COUNT(*), AVG(salary), MAX(salary) FROM employees;");
        defer res.deinit();

        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqual(@as(i64, 3), res.rows[0][0].integer); // COUNT = 3
        try std.testing.expect(res.rows[0][1].real > 95000.0);           // AVG > 95000
        try std.testing.expectEqual(@as(f64, 110000.0), res.rows[0][2].real); // MAX = 110000
    }

    // 5. UPDATE
    {
        var res = try runSql(allocator, &executor, "UPDATE employees SET salary = 99000.0 WHERE id = 1;");
        defer res.deinit();
        try std.testing.expectEqual(@as(usize, 1), res.affected_rows);
    }

    // 6. DELETE
    {
        var res = try runSql(allocator, &executor, "DELETE FROM employees WHERE id = 2;");
        defer res.deinit();
        try std.testing.expectEqual(@as(usize, 1), res.affected_rows);

        // Verify Bob is deleted
        var r_sel = try runSql(allocator, &executor, "SELECT COUNT(*) FROM employees;");
        defer r_sel.deinit();
        try std.testing.expectEqual(@as(i64, 2), r_sel.rows[0][0].integer);
    }
}

test "Multi-Desktop Auto-Sync Simulation with CRDT Convergence" {
    const allocator = std.testing.allocator;

    const dir_desktop_a = "./test_data_desktop_a";
    const dir_desktop_b = "./test_data_desktop_b";
    std.fs.cwd().deleteTree(dir_desktop_a) catch {};
    std.fs.cwd().deleteTree(dir_desktop_b) catch {};
    defer {
        std.fs.cwd().deleteTree(dir_desktop_a) catch {};
        std.fs.cwd().deleteTree(dir_desktop_b) catch {};
    }

    // Initialize two distinct desktop nodes
    const node_a = try Engine.init(allocator, dir_desktop_a, "Desktop-MacBook");
    defer node_a.deinit();

    const node_b = try Engine.init(allocator, dir_desktop_b, "Desktop-iMac");
    defer node_b.deinit();

    // Create same table schema on both
    const cols = [_]Column{
        .{ .name = "id", .data_type = .integer, .primary_key = true, .nullable = false },
        .{ .name = "title", .data_type = .text, .primary_key = false, .nullable = true },
        .{ .name = "status", .data_type = .text, .primary_key = false, .nullable = true },
    };
    const schema = try Schema.init(allocator, &cols);
    defer schema.deinit(allocator);

    _ = try node_a.createTable("tasks", schema);
    _ = try node_b.createTable("tasks", schema);

    // Desktop A inserts Task 1
    var row1 = try Row.init(allocator, 3);
    defer row1.deinit(allocator);
    row1.values[0] = Value{ .integer = 1 };
    row1.values[1] = Value{ .text = try allocator.dupe(u8, "Build Zig Database") };
    row1.values[2] = Value{ .text = try allocator.dupe(u8, "IN_PROGRESS") };
    try node_a.insertRow("tasks", row1);

    // Desktop B inserts Task 2
    var row2 = try Row.init(allocator, 3);
    defer row2.deinit(allocator);
    row2.values[0] = Value{ .integer = 2 };
    row2.values[1] = Value{ .text = try allocator.dupe(u8, "Sync Desktops") };
    row2.values[2] = Value{ .text = try allocator.dupe(u8, "PENDING") };
    try node_b.insertRow("tasks", row2);

    // Replicate changes from A to B
    const changes_a = try node_a.cdc_log.getChangesSince(null, allocator);
    defer {
        for (changes_a) |c| c.deinit(allocator);
        allocator.free(changes_a);
    }
    for (changes_a) |c| {
        _ = try node_b.applyRemoteChange(c);
    }

    // Replicate changes from B to A
    const changes_b = try node_b.cdc_log.getChangesSince(null, allocator);
    defer {
        for (changes_b) |c| c.deinit(allocator);
        allocator.free(changes_b);
    }
    for (changes_b) |c| {
        _ = try node_a.applyRemoteChange(c);
    }

    // Verify both desktops have both Task 1 and Task 2
    const table_a = node_a.getTable("tasks").?;
    const table_b = node_b.getTable("tasks").?;

    try std.testing.expectEqual(@as(usize, 2), table_a.rows.items.len);
    try std.testing.expectEqual(@as(usize, 2), table_b.rows.items.len);

    // Now test concurrent conflict resolution:
    // Desktop A updates Task 1 to COMPLETED
    var row1_updated = try Row.init(allocator, 3);
    defer row1_updated.deinit(allocator);
    row1_updated.values[0] = Value{ .integer = 1 };
    row1_updated.values[1] = Value{ .text = try allocator.dupe(u8, "Build Zig Database") };
    row1_updated.values[2] = Value{ .text = try allocator.dupe(u8, "COMPLETED") };
    try node_a.updateRow("tasks", Value{ .integer = 1 }, row1_updated);

    // Simulate Desktop B being offline and deleting Task 2
    _ = try node_b.deleteRow("tasks", Value{ .integer = 2 });

    // Sync between A and B again
    const delta_a = try node_a.cdc_log.getChangesSince(null, allocator);
    defer {
        for (delta_a) |c| c.deinit(allocator);
        allocator.free(delta_a);
    }
    for (delta_a) |c| {
        _ = try node_b.applyRemoteChange(c);
    }

    const delta_b = try node_b.cdc_log.getChangesSince(null, allocator);
    defer {
        for (delta_b) |c| c.deinit(allocator);
        allocator.free(delta_b);
    }
    for (delta_b) |c| {
        _ = try node_a.applyRemoteChange(c);
    }

    // Both should now have exactly 1 task (Task 1 COMPLETED), and Task 2 deleted
    try std.testing.expectEqual(@as(usize, 1), table_a.rows.items.len);
    try std.testing.expectEqual(@as(usize, 1), table_b.rows.items.len);

    const task_a = try table_a.getRowByPk(Value{ .integer = 1 });
    defer task_a.?.deinit(allocator);
    try std.testing.expectEqualStrings("COMPLETED", task_a.?.values[2].text);

    const task_b = try table_b.getRowByPk(Value{ .integer = 1 });
    defer task_b.?.deinit(allocator);
    try std.testing.expectEqualStrings("COMPLETED", task_b.?.values[2].text);
}

test "Relational ALTER TABLE Operations" {
    const allocator = std.testing.allocator;

    const tmp_dir = "./test_data_alter_sql";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const engine = try Engine.init(allocator, tmp_dir, "TestNodeAlter");
    defer engine.deinit();

    var executor = Executor.init(allocator, engine);

    // 1. CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
    {
        var res = try runSql(allocator, &executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);");
        defer res.deinit();
    }

    // 2. INSERT INTO users VALUES (1, 'Alice', 30);
    {
        var res = try runSql(allocator, &executor, "INSERT INTO users VALUES (1, 'Alice', 30);");
        defer res.deinit();
    }

    // 3. ALTER TABLE users ADD COLUMN email TEXT;
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users ADD COLUMN email TEXT;");
        defer res.deinit();
        try std.testing.expect(res.message != null);
    }

    // Check table schema has 4 columns and existing row has null for email
    {
        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(@as(usize, 4), tbl.schema.columns.len);
        try std.testing.expectEqualStrings("email", tbl.schema.columns[3].name);
        const row = try tbl.getRowByPk(Value{ .integer = 1 });
        defer row.?.deinit(allocator);
        try std.testing.expect(row.?.values[3] == .null_val);
    }

    // 4. UPDATE users SET email = 'alice@example.com' WHERE id = 1;
    {
        var res = try runSql(allocator, &executor, "UPDATE users SET email = 'alice@example.com' WHERE id = 1;");
        defer res.deinit();
    }

    // 5. ALTER TABLE users MODIFY COLUMN age REAL;
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users MODIFY COLUMN age REAL;");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(DataType.real, tbl.schema.columns[2].data_type);
        const row = try tbl.getRowByPk(Value{ .integer = 1 });
        defer row.?.deinit(allocator);
        try std.testing.expect(row.?.values[2] == .real);
        try std.testing.expectEqual(@as(f64, 30.0), row.?.values[2].real);
    }

    // 6. ALTER TABLE users RENAME COLUMN name TO full_name;
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users RENAME COLUMN name TO full_name;");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqualStrings("full_name", tbl.schema.columns[1].name);
    }

    // 7. ALTER TABLE users DROP COLUMN age;
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users DROP COLUMN age;");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(@as(usize, 3), tbl.schema.columns.len);
        try std.testing.expect(tbl.schema.getColumnIndex("age") == null);
    }

    // 8. SELECT id, full_name, email FROM users;
    {
        var res = try runSql(allocator, &executor, "SELECT id, full_name, email FROM users;");
        defer res.deinit();
        try std.testing.expectEqual(@as(usize, 3), res.columns.len);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("Alice", res.rows[0][1].text);
        try std.testing.expectEqualStrings("alice@example.com", res.rows[0][2].text);
    }
}

