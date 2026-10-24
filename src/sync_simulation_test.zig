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
const protocol = @import("sync/protocol.zig");
const Parser = @import("sql/parser.zig").Parser;
const Executor = @import("sql/executor.zig").Executor;
pub const mariadb = @import("compat/mariadb.zig");
pub const storage_table = @import("storage/table.zig");

test {
    std.testing.refAllDecls(mariadb);
    std.testing.refAllDecls(storage_table);
}

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

    // 9. ALTER TABLE users DROP PRIMARY KEY;
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users DROP PRIMARY KEY;");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(@as(?usize, null), tbl.schema.getPrimaryKeyIndex());
    }

    // 10. ALTER TABLE users ADD PRIMARY KEY (email);
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users ADD PRIMARY KEY (email);");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(@as(?usize, 2), tbl.schema.getPrimaryKeyIndex());
        try std.testing.expectEqualStrings("email", tbl.schema.columns[tbl.schema.getPrimaryKeyIndex().?].name);
        try std.testing.expect(tbl.pk_index.contains("alice@example.com"));
    }

    // 11. ALTER TABLE users ADD PRIMARY KEY (id); (redefine back to id)
    {
        var res = try runSql(allocator, &executor, "ALTER TABLE users ADD PRIMARY KEY (id);");
        defer res.deinit();
        try std.testing.expect(res.message != null);

        const tbl = engine.getTable("users").?;
        try std.testing.expectEqual(@as(?usize, 0), tbl.schema.getPrimaryKeyIndex());
        try std.testing.expectEqualStrings("id", tbl.schema.columns[tbl.schema.getPrimaryKeyIndex().?].name);
        try std.testing.expect(tbl.pk_index.contains("1"));
        try std.testing.expect(!tbl.pk_index.contains("alice@example.com"));
    }
}

test "Composite Primary Key Engine and SQL Operations" {
    const allocator = std.testing.allocator;

    const tmp_dir = "./test_data_composite_pk";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const engine = try Engine.init(allocator, tmp_dir, "TestNode");
    defer engine.deinit();

    var executor = Executor.init(allocator, engine);

    const cols = try allocator.alloc(Column, 3);
    cols[0] = Column{ .name = try allocator.dupe(u8, "company_code"), .data_type = .text, .primary_key = true, .nullable = false };
    cols[1] = Column{ .name = try allocator.dupe(u8, "account_id"), .data_type = .integer, .primary_key = true, .nullable = false };
    cols[2] = Column{ .name = try allocator.dupe(u8, "balance"), .data_type = .real, .primary_key = false, .nullable = true };
    const schema = Schema{ .columns = cols };
    defer schema.deinit(allocator);
    _ = try engine.createTable("accounts", schema);

    // Insert row 1: ("1000", 101, 500.5)
    var r1_vals = try allocator.alloc(Value, 3);
    r1_vals[0] = Value{ .text = try allocator.dupe(u8, "1000") };
    r1_vals[1] = Value{ .integer = 101 };
    r1_vals[2] = Value{ .real = 500.5 };
    const r1 = Row{ .values = r1_vals };
    defer r1.deinit(allocator);
    try engine.insertRow("accounts", r1);

    // Insert row 2: ("1000", 102, 300.0) -> same company_code, different account_id
    var r2_vals = try allocator.alloc(Value, 3);
    r2_vals[0] = Value{ .text = try allocator.dupe(u8, "1000") };
    r2_vals[1] = Value{ .integer = 102 };
    r2_vals[2] = Value{ .real = 300.0 };
    const r2 = Row{ .values = r2_vals };
    defer r2.deinit(allocator);
    try engine.insertRow("accounts", r2);

    // Insert row 3: ("2000", 101, 750.0) -> different company_code, same account_id
    var r3_vals = try allocator.alloc(Value, 3);
    r3_vals[0] = Value{ .text = try allocator.dupe(u8, "2000") };
    r3_vals[1] = Value{ .integer = 101 };
    r3_vals[2] = Value{ .real = 750.0 };
    const r3 = Row{ .values = r3_vals };
    defer r3.deinit(allocator);
    try engine.insertRow("accounts", r3);

    // Duplicate ("1000", 101) -> duplicate error
    var r_dup_vals = try allocator.alloc(Value, 3);
    r_dup_vals[0] = Value{ .text = try allocator.dupe(u8, "1000") };
    r_dup_vals[1] = Value{ .integer = 101 };
    r_dup_vals[2] = Value{ .real = 999.0 };
    const r_dup = Row{ .values = r_dup_vals };
    defer r_dup.deinit(allocator);
    try std.testing.expectError(error.DuplicatePrimaryKey, engine.insertRow("accounts", r_dup));

    // Update row by index
    var r1_mod_vals = try allocator.alloc(Value, 3);
    r1_mod_vals[0] = Value{ .text = try allocator.dupe(u8, "1000") };
    r1_mod_vals[1] = Value{ .integer = 101 };
    r1_mod_vals[2] = Value{ .real = 1200.0 };
    const r1_mod = Row{ .values = r1_mod_vals };
    defer r1_mod.deinit(allocator);
    try engine.updateRowByIndex("accounts", 0, r1_mod);

    // SQL Select
    {
        var res = try runSql(allocator, &executor, "SELECT company_code, account_id, balance FROM accounts;");
        defer res.deinit();
        try std.testing.expectEqual(@as(usize, 3), res.rows.len);
    }

    // SQL Delete
    {
        var res = try runSql(allocator, &executor, "DELETE FROM accounts WHERE account_id = 102;");
        defer res.deinit();
        try std.testing.expectEqual(@as(usize, 1), res.affected_rows);
    }

    // Engine deleteRowByIndex
    const deleted = try engine.deleteRowByIndex("accounts", 0);
    try std.testing.expect(deleted);
}

test "Automatic Table Schema and Data Synchronization Between Desktops" {
    const allocator = std.testing.allocator;

    const dir_a = "./test_peer_a";
    const dir_b = "./test_peer_b";
    std.fs.cwd().deleteTree(dir_a) catch {};
    std.fs.cwd().deleteTree(dir_b) catch {};
    defer std.fs.cwd().deleteTree(dir_a) catch {};
    defer std.fs.cwd().deleteTree(dir_b) catch {};

    const node_a = try Engine.init(allocator, dir_a, "Desktop-A");
    defer node_a.deinit();

    const node_b = try Engine.init(allocator, dir_b, "Desktop-B");
    defer node_b.deinit();

    // 1. Node A creates table 'customers' with schema, metadata, and data
    const cols = [_]Column{
        .{ .name = "id", .data_type = .integer, .primary_key = true, .auto_increment = true, .nullable = false },
        .{ .name = "name", .data_type = .text, .primary_key = false, .auto_increment = false, .nullable = false },
        .{ .name = "balance", .data_type = .real, .primary_key = false, .auto_increment = false, .nullable = true },
    };
    const schema = try Schema.init(allocator, &cols);
    defer schema.deinit(allocator);
    _ = try node_a.createTable("customers", schema);
    try node_a.setTableDescription("customers", "Customer Directory");
    try node_a.setColumnDescription("customers", "balance", "Current ledger balance");

    // Insert 2 rows on Node A
    var r1_vals = try allocator.alloc(Value, 3);
    r1_vals[0] = Value{ .integer = 1 };
    r1_vals[1] = Value{ .text = try allocator.dupe(u8, "Alice Corp") };
    r1_vals[2] = Value{ .real = 1500.50 };
    const r1 = Row{ .values = r1_vals };
    defer r1.deinit(allocator);
    try node_a.insertRow("customers", r1);

    var r2_vals = try allocator.alloc(Value, 3);
    r2_vals[0] = Value{ .integer = 2 };
    r2_vals[1] = Value{ .text = try allocator.dupe(u8, "Bob Logistics") };
    r2_vals[2] = Value{ .real = 3200.00 };
    const r2 = Row{ .values = r2_vals };
    defer r2.deinit(allocator);
    try node_a.insertRow("customers", r2);

    // 2. Verify Node B is initially completely empty (0 tables)
    {
        const b_tables = try node_b.listTables(allocator);
        defer {
            for (b_tables) |t| allocator.free(t);
            allocator.free(b_tables);
        }
        try std.testing.expectEqual(@as(usize, 0), b_tables.len);
    }

    // 3. Generate snapshot on Node A and simulate wire transmission to Node B
    const snap_opt = try node_a.createTableSnapshot("customers", allocator);
    try std.testing.expect(snap_opt != null);
    var snap = snap_opt.?;
    defer snap.deinit(allocator);

    var wire_buf = std.ArrayList(u8).init(allocator);
    defer wire_buf.deinit();
    try snap.serialize(wire_buf.writer());

    // 4. Node B receives wire payload, deserializes, and applies snapshot
    var fbs = std.io.fixedBufferStream(wire_buf.items);
    var remote_snap = try protocol.TableSnapshot.deserialize(allocator, fbs.reader());
    defer remote_snap.deinit(allocator);

    try node_b.applyTableSnapshot(remote_snap);

    // 5. Verify Node B now has 'customers' table created with exact schema and data
    {
        const b_tables = try node_b.listTables(allocator);
        defer {
            for (b_tables) |t| allocator.free(t);
            allocator.free(b_tables);
        }
        try std.testing.expectEqual(@as(usize, 1), b_tables.len);
        try std.testing.expectEqualStrings("customers", b_tables[0]);
    }

    const table_b = node_b.getTable("customers").?;
    try std.testing.expectEqual(@as(usize, 3), table_b.schema.columns.len);
    try std.testing.expectEqualStrings("id", table_b.schema.columns[0].name);
    try std.testing.expect(table_b.schema.columns[0].primary_key);
    try std.testing.expectEqualStrings("name", table_b.schema.columns[1].name);
    try std.testing.expectEqualStrings("balance", table_b.schema.columns[2].name);

    // Verify metadata transferred to Node B
    try std.testing.expectEqualStrings("Customer Directory", node_b.metadata.getTableDescription("customers"));
    try std.testing.expectEqualStrings("Current ledger balance", node_b.metadata.getColumnDescription("customers", "balance"));

    // Verify row data on Node B
    try std.testing.expectEqual(@as(usize, 2), table_b.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), table_b.rows.items[0].values[0].integer);
    try std.testing.expectEqualStrings("Alice Corp", table_b.rows.items[0].values[1].text);
    try std.testing.expectEqual(@as(f64, 1500.50), table_b.rows.items[0].values[2].real);
    try std.testing.expectEqual(@as(i64, 2), table_b.rows.items[1].values[0].integer);
    try std.testing.expectEqualStrings("Bob Logistics", table_b.rows.items[1].values[1].text);

    // 6. Test DDL alteration propagation (e.g. adding column)
    const new_col = Column{ .name = "status", .data_type = .text, .primary_key = false, .auto_increment = false, .nullable = true };
    try node_a.alterTableAddColumn("customers", new_col);

    const snap2_opt = try node_a.createTableSnapshot("customers", allocator);
    var snap2 = snap2_opt.?;
    defer snap2.deinit(allocator);

    var wire_buf2 = std.ArrayList(u8).init(allocator);
    defer wire_buf2.deinit();
    try snap2.serialize(wire_buf2.writer());

    var fbs2 = std.io.fixedBufferStream(wire_buf2.items);
    var remote_snap2 = try protocol.TableSnapshot.deserialize(allocator, fbs2.reader());
    defer remote_snap2.deinit(allocator);

    try node_b.applyTableSnapshot(remote_snap2);

    // Verify Node B schema has been updated with the 4th column
    const table_b_updated = node_b.getTable("customers").?;
    try std.testing.expectEqual(@as(usize, 4), table_b_updated.schema.columns.len);
    try std.testing.expectEqualStrings("status", table_b_updated.schema.columns[3].name);
}



