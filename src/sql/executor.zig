const std = @import("std");
const types = @import("../types.zig");
const Value = types.Value;
const Row = types.Row;
const Schema = types.Schema;
const Column = types.Column;
const ast = @import("ast.zig");
const Statement = ast.Statement;
const SelectItem = ast.SelectItem;
const AggregateType = ast.AggregateType;
const Expr = ast.Expr;
const BinaryOp = ast.BinaryOp;
const Engine = @import("../storage/engine.zig").Engine;
const Table = @import("../storage/table.zig").Table;

pub const QueryResult = struct {
    allocator: std.mem.Allocator,
    columns: [][]const u8,
    rows: [][]Value,
    affected_rows: usize = 0,
    message: ?[]const u8 = null,
    execution_time_ns: u64 = 0,

    pub fn deinit(self: *QueryResult) void {
        for (self.columns) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.columns);

        for (self.rows) |row| {
            for (row) |val| {
                val.deinit(self.allocator);
            }
            self.allocator.free(row);
        }
        self.allocator.free(self.rows);

        if (self.message) |msg| {
            self.allocator.free(msg);
        }
    }

    pub fn renderAsciiTable(self: QueryResult, writer: anytype) !void {
        if (self.message) |msg| {
            try writer.print("{s}\n", .{msg});
        }

        if (self.columns.len == 0) {
            try writer.print("Time: {d:.2} ms\n", .{@as(f64, @floatFromInt(self.execution_time_ns)) / 1_000_000.0});
            return;
        }

        // Calculate column widths
        var widths = try self.allocator.alloc(usize, self.columns.len);
        defer self.allocator.free(widths);

        for (self.columns, 0..) |col, i| {
            widths[i] = @max(col.len, 4);
        }

        for (self.rows) |row| {
            for (row, 0..) |val, i| {
                const str = try val.toStringAlloc(self.allocator);
                defer self.allocator.free(str);
                widths[i] = @max(widths[i], str.len);
            }
        }

        // Top border: +-------+-------+
        try writer.writeAll("+");
        for (widths) |w| {
            var i: usize = 0;
            while (i < w + 2) : (i += 1) try writer.writeByte('-');
            try writer.writeAll("+");
        }
        try writer.writeAll("\n");

        // Header: | col1  | col2  |
        try writer.writeAll("|");
        for (self.columns, 0..) |col, i| {
            try writer.print(" {s}", .{col});
            var pad = widths[i] - col.len;
            while (pad > 0) : (pad -= 1) try writer.writeByte(' ');
            try writer.writeAll(" |");
        }
        try writer.writeAll("\n");

        // Middle border: +-------+-------+
        try writer.writeAll("+");
        for (widths) |w| {
            var i: usize = 0;
            while (i < w + 2) : (i += 1) try writer.writeByte('-');
            try writer.writeAll("+");
        }
        try writer.writeAll("\n");

        // Rows
        for (self.rows) |row| {
            try writer.writeAll("|");
            for (row, 0..) |val, i| {
                const str = try val.toStringAlloc(self.allocator);
                defer self.allocator.free(str);
                try writer.print(" {s}", .{str});
                var pad = widths[i] - str.len;
                while (pad > 0) : (pad -= 1) try writer.writeByte(' ');
                try writer.writeAll(" |");
            }
            try writer.writeAll("\n");
        }

        // Bottom border: +-------+-------+
        try writer.writeAll("+");
        for (widths) |w| {
            var i: usize = 0;
            while (i < w + 2) : (i += 1) try writer.writeByte('-');
            try writer.writeAll("+");
        }
        try writer.writeAll("\n");

        try writer.print("({d} rows in {d:.2} ms)\n", .{
            self.rows.len,
            @as(f64, @floatFromInt(self.execution_time_ns)) / 1_000_000.0,
        });
    }
};

pub const Executor = struct {
    allocator: std.mem.Allocator,
    engine: *Engine,

    pub fn init(allocator: std.mem.Allocator, engine: *Engine) Executor {
        return Executor{
            .allocator = allocator,
            .engine = engine,
        };
    }

    pub fn execute(self: *Executor, stmt: Statement) !QueryResult {
        const start_time = std.time.nanoTimestamp();
        var result = try self.executeInternal(stmt);
        const end_time = std.time.nanoTimestamp();
        result.execution_time_ns = @intCast(@max(0, end_time - start_time));
        return result;
    }

    fn executeInternal(self: *Executor, stmt: Statement) !QueryResult {
        switch (stmt) {
            .create_table => |c| return try self.execCreateTable(c),
            .alter_table => |a| return try self.execAlterTable(a),
            .drop_table => |d| return try self.execDropTable(d),
            .insert => |ins| return try self.execInsert(ins),
            .select => |s| return try self.execSelect(s),
            .update => |u| return try self.execUpdate(u),
            .delete => |d| return try self.execDelete(d),
            .show_tables => return try self.execShowTables(),
            .describe => |name| return try self.execDescribe(name),
            .status => return try self.execStatus(),
            .help => return try self.execHelp(),
            .peers => return try self.execPeers(),
            .sync_connect => |sc| return try self.execSyncConnect(sc),
            .exit => {
                return QueryResult{
                    .allocator = self.allocator,
                    .columns = &[_][]const u8{},
                    .rows = &[_][]Value{},
                    .message = try self.allocator.dupe(u8, "Goodbye!"),
                };
            },
        }
    }

    fn execAlterTable(self: *Executor, a: ast.AlterTableStmt) !QueryResult {
        var msg: []const u8 = undefined;
        switch (a.action) {
            .add_column => |col| {
                try self.engine.alterTableAddColumn(a.table_name, col);
                msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' column '{s}' added successfully.", .{ a.table_name, col.name });
            },
            .drop_column => |col_name| {
                try self.engine.alterTableDropColumn(a.table_name, col_name);
                msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' column '{s}' dropped successfully.", .{ a.table_name, col_name });
            },
            .modify_column => |col| {
                try self.engine.alterTableModifyColumn(a.table_name, col);
                msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' column '{s}' modified successfully.", .{ a.table_name, col.name });
            },
            .rename_column => |ren| {
                try self.engine.alterTableRenameColumn(a.table_name, ren.old_name, ren.new_name);
                msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' column '{s}' renamed to '{s}' successfully.", .{ a.table_name, ren.old_name, ren.new_name });
            },
        }

        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .message = msg,
        };
    }

    fn execCreateTable(self: *Executor, c: ast.CreateTableStmt) !QueryResult {
        const schema = try Schema.init(self.allocator, c.columns);
        defer schema.deinit(self.allocator);

        _ = try self.engine.createTable(c.table_name, schema);

        const msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' created successfully.", .{c.table_name});
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .message = msg,
        };
    }

    fn execDropTable(self: *Executor, d: ast.DropTableStmt) !QueryResult {
        const dropped = try self.engine.dropTable(d.table_name);
        const msg = if (dropped)
            try std.fmt.allocPrint(self.allocator, "Table '{s}' dropped.", .{d.table_name})
        else
            try std.fmt.allocPrint(self.allocator, "Table '{s}' does not exist.", .{d.table_name});

        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .message = msg,
        };
    }

    fn execInsert(self: *Executor, ins: ast.InsertStmt) !QueryResult {
        const table = self.engine.getTable(ins.table_name) orelse return error.TableNotFound;

        var full_row = try Row.init(self.allocator, table.schema.columns.len);
        errdefer full_row.deinit(self.allocator);

        if (ins.columns) |cols| {
            if (cols.len != ins.values.len) return error.ColumnCountMismatch;
            for (cols, 0..) |col_name, i| {
                const col_idx = table.schema.getColumnIndex(col_name) orelse return error.ColumnNotFound;
                full_row.values[col_idx] = try ins.values[i].clone(self.allocator);
            }
        } else {
            if (ins.values.len != table.schema.columns.len) return error.ColumnCountMismatch;
            for (ins.values, 0..) |v, i| {
                full_row.values[i] = try v.clone(self.allocator);
            }
        }

        try self.engine.insertRow(ins.table_name, full_row);
        full_row.deinit(self.allocator);

        const msg = try std.fmt.allocPrint(self.allocator, "INSERT 0 1", .{});
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .affected_rows = 1,
            .message = msg,
        };
    }

    fn execSelect(self: *Executor, s: ast.SelectStmt) !QueryResult {
        const table = self.engine.getTable(s.from_table) orelse return error.TableNotFound;

        table.mutex.lock();
        defer table.mutex.unlock();

        var matched_rows = std.ArrayList(Row).init(self.allocator);
        defer matched_rows.deinit();

        for (table.rows.items) |row| {
            if (s.where) |where_expr| {
                if (!try self.evalFilter(row, table.schema, where_expr)) {
                    continue;
                }
            }
            try matched_rows.append(row);
        }

        // Handle ORDER BY
        if (s.order_by) |ord| {
            const col_idx = table.schema.getColumnIndex(ord.column_name) orelse return error.ColumnNotFound;
            const sort_ctx = struct {
                idx: usize,
                asc: bool,
                pub fn lessThan(ctx: @This(), a: Row, b: Row) bool {
                    const cmp = a.values[ctx.idx].compare(b.values[ctx.idx]);
                    if (ctx.asc) return cmp == .lt;
                    return cmp == .gt;
                }
            }{ .idx = col_idx, .asc = ord.ascending };

            std.mem.sort(Row, matched_rows.items, sort_ctx, struct {
                fn order(ctx: @TypeOf(sort_ctx), a: Row, b: Row) bool {
                    return ctx.lessThan(a, b);
                }
            }.order);
        }

        // Handle LIMIT
        var count_to_take = matched_rows.items.len;
        if (s.limit) |lim| {
            count_to_take = @min(count_to_take, lim);
        }

        // Check for aggregate query (e.g. COUNT(*), AVG(score))
        var has_aggregates = false;
        for (s.items) |item| {
            if (item.aggregate != null) {
                has_aggregates = true;
                break;
            }
        }

        if (has_aggregates) {
            return try self.evalAggregates(s.items, table.schema, matched_rows.items[0..count_to_take]);
        }

        // Regular projection
        var col_names = std.ArrayList([]const u8).init(self.allocator);
        var col_indices = std.ArrayList(usize).init(self.allocator);
        defer col_indices.deinit();

        for (s.items) |item| {
            if (item.star) {
                for (table.schema.columns, 0..) |col, idx| {
                    try col_names.append(try self.allocator.dupe(u8, col.name));
                    try col_indices.append(idx);
                }
            } else if (item.column_name) |cname| {
                const idx = table.schema.getColumnIndex(cname) orelse return error.ColumnNotFound;
                try col_names.append(try self.allocator.dupe(u8, cname));
                try col_indices.append(idx);
            }
        }

        var result_rows = std.ArrayList([]Value).init(self.allocator);
        errdefer result_rows.deinit();

        for (matched_rows.items[0..count_to_take]) |row| {
            var row_vals = try self.allocator.alloc(Value, col_indices.items.len);
            for (col_indices.items, 0..) |c_idx, i| {
                row_vals[i] = try row.values[c_idx].clone(self.allocator);
            }
            try result_rows.append(row_vals);
        }

        return QueryResult{
            .allocator = self.allocator,
            .columns = try col_names.toOwnedSlice(),
            .rows = try result_rows.toOwnedSlice(),
        };
    }

    fn evalAggregates(
        self: *Executor,
        items: []SelectItem,
        schema: Schema,
        rows: []Row,
    ) !QueryResult {
        var col_names = std.ArrayList([]const u8).init(self.allocator);
        var out_vals = try self.allocator.alloc(Value, items.len);

        for (items, 0..) |item, i| {
            const agg = item.aggregate orelse .count;
            switch (agg) {
                .count => {
                    try col_names.append(try self.allocator.dupe(u8, "COUNT"));
                    out_vals[i] = Value{ .integer = @intCast(rows.len) };
                },
                .sum, .avg => {
                    const cname = item.column_name orelse "*";
                    const col_idx = schema.getColumnIndex(cname) orelse return error.ColumnNotFound;
                    try col_names.append(try std.fmt.allocPrint(self.allocator, "{s}({s})", .{
                        if (agg == .sum) "SUM" else "AVG",
                        cname,
                    }));

                    var sum: f64 = 0.0;
                    for (rows) |row| {
                        switch (row.values[col_idx]) {
                            .integer => |iv| sum += @floatFromInt(iv),
                            .real => |rv| sum += rv,
                            else => {},
                        }
                    }
                    if (agg == .sum) {
                        out_vals[i] = Value{ .real = sum };
                    } else {
                        const avg = if (rows.len > 0) sum / @as(f64, @floatFromInt(rows.len)) else 0.0;
                        out_vals[i] = Value{ .real = avg };
                    }
                },
                .min, .max => {
                    const cname = item.column_name orelse "*";
                    const col_idx = schema.getColumnIndex(cname) orelse return error.ColumnNotFound;
                    try col_names.append(try std.fmt.allocPrint(self.allocator, "{s}({s})", .{
                        if (agg == .min) "MIN" else "MAX",
                        cname,
                    }));

                    if (rows.len == 0) {
                        out_vals[i] = .null_val;
                    } else {
                        var best = rows[0].values[col_idx];
                        for (rows[1..]) |row| {
                            const val = row.values[col_idx];
                            const cmp = val.compare(best);
                            if (agg == .min and cmp == .lt) best = val;
                            if (agg == .max and cmp == .gt) best = val;
                        }
                        out_vals[i] = try best.clone(self.allocator);
                    }
                },
            }
        }

        var result_rows = try self.allocator.alloc([]Value, 1);
        result_rows[0] = out_vals;

        return QueryResult{
            .allocator = self.allocator,
            .columns = try col_names.toOwnedSlice(),
            .rows = result_rows,
        };
    }

    fn evalFilter(self: *Executor, row: Row, schema: Schema, expr: *Expr) !bool {
        switch (expr.*) {
            .binary => |b| {
                if (b.op == .and_op) {
                    return (try self.evalFilter(row, schema, b.left)) and (try self.evalFilter(row, schema, b.right));
                } else if (b.op == .or_op) {
                    return (try self.evalFilter(row, schema, b.left)) or (try self.evalFilter(row, schema, b.right));
                }

                const left_val = try self.evalValue(row, schema, b.left);
                const right_val = try self.evalValue(row, schema, b.right);
                const cmp = left_val.compare(right_val);

                return switch (b.op) {
                    .eq => cmp == .eq,
                    .neq => cmp != .eq,
                    .lt => cmp == .lt,
                    .lte => cmp == .lt or cmp == .eq,
                    .gt => cmp == .gt,
                    .gte => cmp == .gt or cmp == .eq,
                    else => false,
                };
            },
            .literal => |v| return v == .boolean and v.boolean,
            .column => |col_ref| {
                const idx = schema.getColumnIndex(col_ref.column_name) orelse return false;
                const v = row.values[idx];
                return v == .boolean and v.boolean;
            },
        }
    }

    fn evalValue(self: *Executor, row: Row, schema: Schema, expr: *Expr) !Value {
        _ = self;
        switch (expr.*) {
            .literal => |v| return v,
            .column => |col_ref| {
                const idx = schema.getColumnIndex(col_ref.column_name) orelse return error.ColumnNotFound;
                return row.values[idx];
            },
            .binary => return error.UnsupportedExpression,
        }
    }

    fn execUpdate(self: *Executor, u: ast.UpdateStmt) !QueryResult {
        const table = self.engine.getTable(u.table_name) orelse return error.TableNotFound;

        var pks_to_update = std.ArrayList(Value).init(self.allocator);
        defer pks_to_update.deinit();

        table.mutex.lock();
        for (table.rows.items) |row| {
            if (u.where) |where_expr| {
                if (!try self.evalFilter(row, table.schema, where_expr)) {
                    continue;
                }
            }
            const pk = try table.getRowPrimaryKey(row);
            try pks_to_update.append(pk);
        }
        table.mutex.unlock();

        var affected: usize = 0;
        for (pks_to_update.items) |pk| {
            if (try table.getRowByPk(pk)) |old_row| {
                defer old_row.deinit(self.allocator);
                var new_row = try old_row.clone(self.allocator);
                errdefer new_row.deinit(self.allocator);

                for (u.assignments) |assign| {
                    const c_idx = table.schema.getColumnIndex(assign.column_name) orelse continue;
                    new_row.values[c_idx].deinit(self.allocator);
                    new_row.values[c_idx] = try assign.value.clone(self.allocator);
                }

                try self.engine.updateRow(u.table_name, pk, new_row);
                new_row.deinit(self.allocator);
                affected += 1;
            }
        }

        const msg = try std.fmt.allocPrint(self.allocator, "UPDATE {d}", .{affected});
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .affected_rows = affected,
            .message = msg,
        };
    }

    fn execDelete(self: *Executor, d: ast.DeleteStmt) !QueryResult {
        const table = self.engine.getTable(d.table_name) orelse return error.TableNotFound;

        var pks_to_delete = std.ArrayList(Value).init(self.allocator);
        defer pks_to_delete.deinit();

        table.mutex.lock();
        for (table.rows.items) |row| {
            if (d.where) |where_expr| {
                if (!try self.evalFilter(row, table.schema, where_expr)) {
                    continue;
                }
            }
            const pk = try table.getRowPrimaryKey(row);
            try pks_to_delete.append(pk);
        }
        table.mutex.unlock();

        var affected: usize = 0;
        for (pks_to_delete.items) |pk| {
            if (try self.engine.deleteRow(d.table_name, pk)) {
                affected += 1;
            }
        }

        const msg = try std.fmt.allocPrint(self.allocator, "DELETE {d}", .{affected});
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .affected_rows = affected,
            .message = msg,
        };
    }

    fn execShowTables(self: *Executor) !QueryResult {
        const table_names = try self.engine.listTables(self.allocator);
        defer {
            for (table_names) |tn| self.allocator.free(tn);
            self.allocator.free(table_names);
        }

        var col_names = try self.allocator.alloc([]const u8, 1);
        col_names[0] = try self.allocator.dupe(u8, "table_name");

        var rows = try self.allocator.alloc([]Value, table_names.len);
        for (table_names, 0..) |tn, i| {
            rows[i] = try self.allocator.alloc(Value, 1);
            rows[i][0] = Value{ .text = try self.allocator.dupe(u8, tn) };
        }

        return QueryResult{
            .allocator = self.allocator,
            .columns = col_names,
            .rows = rows,
        };
    }

    fn execDescribe(self: *Executor, table_name: []const u8) !QueryResult {
        const table = self.engine.getTable(table_name) orelse return error.TableNotFound;

        var col_names = try self.allocator.alloc([]const u8, 4);
        col_names[0] = try self.allocator.dupe(u8, "column_name");
        col_names[1] = try self.allocator.dupe(u8, "type");
        col_names[2] = try self.allocator.dupe(u8, "primary_key");
        col_names[3] = try self.allocator.dupe(u8, "nullable");

        var rows = try self.allocator.alloc([]Value, table.schema.columns.len);
        for (table.schema.columns, 0..) |col, i| {
            rows[i] = try self.allocator.alloc(Value, 4);
            rows[i][0] = Value{ .text = try self.allocator.dupe(u8, col.name) };
            rows[i][1] = Value{ .text = try self.allocator.dupe(u8, col.data_type.name()) };
            rows[i][2] = Value{ .boolean = col.primary_key };
            rows[i][3] = Value{ .boolean = col.nullable };
        }

        return QueryResult{
            .allocator = self.allocator,
            .columns = col_names,
            .rows = rows,
        };
    }

    fn execStatus(self: *Executor) !QueryResult {
        var col_names = try self.allocator.alloc([]const u8, 2);
        col_names[0] = try self.allocator.dupe(u8, "metric");
        col_names[1] = try self.allocator.dupe(u8, "value");

        const hlc_str = try self.engine.hlc.toStringAlloc(self.allocator);
        defer self.allocator.free(hlc_str);

        var node_hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&node_hex, "{s}", .{std.fmt.fmtSliceHexLower(&self.engine.node_id)}) catch "";

        const tables_count = self.engine.tables.count();
        const cdc_count = self.engine.cdc_log.count();

        var rows = try self.allocator.alloc([]Value, 5);

        rows[0] = try self.allocator.alloc(Value, 2);
        rows[0][0] = Value{ .text = try self.allocator.dupe(u8, "Desktop Node ID") };
        rows[0][1] = Value{ .text = try self.allocator.dupe(u8, &node_hex) };

        rows[1] = try self.allocator.alloc(Value, 2);
        rows[1][0] = Value{ .text = try self.allocator.dupe(u8, "Desktop Name") };
        rows[1][1] = Value{ .text = try self.allocator.dupe(u8, self.engine.desktop_name) };

        rows[2] = try self.allocator.alloc(Value, 2);
        rows[2][0] = Value{ .text = try self.allocator.dupe(u8, "Hybrid Logical Clock") };
        rows[2][1] = Value{ .text = try self.allocator.dupe(u8, hlc_str) };

        rows[3] = try self.allocator.alloc(Value, 2);
        rows[3][0] = Value{ .text = try self.allocator.dupe(u8, "Active Tables") };
        rows[3][1] = Value{ .integer = @intCast(tables_count) };

        rows[4] = try self.allocator.alloc(Value, 2);
        rows[4][0] = Value{ .text = try self.allocator.dupe(u8, "Replication Log Size") };
        rows[4][1] = Value{ .integer = @intCast(cdc_count) };

        return QueryResult{
            .allocator = self.allocator,
            .columns = col_names,
            .rows = rows,
        };
    }

    fn execPeers(self: *Executor) !QueryResult {
        var col_names = try self.allocator.alloc([]const u8, 4);
        col_names[0] = try self.allocator.dupe(u8, "peer_node_id");
        col_names[1] = try self.allocator.dupe(u8, "desktop_name");
        col_names[2] = try self.allocator.dupe(u8, "address");
        col_names[3] = try self.allocator.dupe(u8, "status");

        return QueryResult{
            .allocator = self.allocator,
            .columns = col_names,
            .rows = &[_][]Value{},
            .message = try self.allocator.dupe(u8, "No external desktop peers actively connected yet."),
        };
    }

    fn execSyncConnect(self: *Executor, sc: ast.SyncConnectStmt) !QueryResult {
        const msg = try std.fmt.allocPrint(self.allocator, "Connecting to desktop peer at {s}...", .{sc.address});
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .message = msg,
        };
    }

    fn execHelp(self: *Executor) !QueryResult {
        const help_text =
            \\StrataDB Interactive Relational Commands:
            \\  CREATE TABLE <table> (<col> <type> [PRIMARY KEY], ...);
            \\  ALTER TABLE <table> ADD [COLUMN] <col> <type>;
            \\  ALTER TABLE <table> DROP [COLUMN] <col>;
            \\  ALTER TABLE <table> MODIFY [COLUMN] <col> <type>;
            \\  ALTER TABLE <table> RENAME [COLUMN] <old> TO <new>;
            \\  INSERT INTO <table> [(cols...)] VALUES (vals...);
            \\  SELECT <cols> FROM <table> [WHERE <expr>] [ORDER BY <col> [ASC|DESC]] [LIMIT <n>];
            \\  SELECT COUNT(*), AVG(col), SUM(col), MIN(col), MAX(col) FROM <table>;
            \\  UPDATE <table> SET <col> = <val> [WHERE <expr>];
            \\  DELETE FROM <table> [WHERE <expr>];
            \\  SHOW TABLES;
            \\  DESCRIBE <table>;
            \\  STATUS;
            \\  PEERS;
            \\  SYNC <ip:port>;
            \\  HELP;
            \\  EXIT;
        ;
        return QueryResult{
            .allocator = self.allocator,
            .columns = &[_][]const u8{},
            .rows = &[_][]Value{},
            .message = try self.allocator.dupe(u8, help_text),
        };
    }
};
