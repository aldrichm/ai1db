const std = @import("std");
const types = @import("../types.zig");
const Engine = @import("../storage/engine.zig").Engine;
const Table = @import("../storage/table.zig").Table;
const Schema = types.Schema;
const Column = types.Column;
const DataType = types.DataType;
const Row = types.Row;
const Value = types.Value;

pub const ImportResult = struct {
    tables_created: usize = 0,
    rows_inserted: usize = 0,
    statements_processed: usize = 0,
    statements_skipped: usize = 0,
    errors: usize = 0,
};

pub fn unquoteIdentifier(name: []const u8) []const u8 {
    const trimmed = std.mem.trim(u8, name, " \t\r\n");
    if (trimmed.len >= 2 and trimmed[0] == '`' and trimmed[trimmed.len - 1] == '`') {
        return trimmed[1 .. trimmed.len - 1];
    }
    if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') {
        return trimmed[1 .. trimmed.len - 1];
    }
    return trimmed;
}

pub fn unescapeSqlString(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (std.mem.indexOfScalar(u8, raw, '\\') == null and std.mem.indexOfScalar(u8, raw, '\'') == null) {
        return try allocator.dupe(u8, raw);
    }

    var out = try std.ArrayList(u8).initCapacity(allocator, raw.len);
    errdefer out.deinit();

    var i: usize = 0;
    while (i < raw.len) {
        if (raw[i] == '\\' and i + 1 < raw.len) {
            i += 1;
            switch (raw[i]) {
                'n' => try out.append('\n'),
                'r' => try out.append('\r'),
                't' => try out.append('\t'),
                '0' => try out.append(0),
                'Z' => try out.append(26),
                '\'' => try out.append('\''),
                '"' => try out.append('"'),
                '\\' => try out.append('\\'),
                else => |c| try out.append(c),
            }
        } else if (raw[i] == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') {
            try out.append('\'');
            i += 1;
        } else {
            try out.append(raw[i]);
        }
        i += 1;
    }
    return out.toOwnedSlice();
}

fn isLeapYear(year: i64) bool {
    return (@mod(year, 4) == 0 and @mod(year, 100) != 0) or (@mod(year, 400) == 0);
}

fn daysBeforeMonth(year: i64, month: usize) i64 {
    const normal = [_]i64{ 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
    const leap = [_]i64{ 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 };
    const table = if (isLeapYear(year)) &leap else &normal;
    if (month >= 1 and month <= 12) {
        return table[month - 1];
    }
    return 0;
}

pub fn parseSqlDateTimeToMillis(str: []const u8) ?i64 {
    if (str.len < 10) return null;
    const year = std.fmt.parseInt(i64, str[0..4], 10) catch return null;
    const month = std.fmt.parseInt(usize, str[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, str[8..10], 10) catch return null;

    var hour: i64 = 0;
    var min: i64 = 0;
    var sec: i64 = 0;
    if (str.len >= 16) {
        hour = std.fmt.parseInt(i64, str[11..13], 10) catch 0;
        min = std.fmt.parseInt(i64, str[14..16], 10) catch 0;
        if (str.len >= 19) {
            sec = std.fmt.parseInt(i64, str[17..19], 10) catch 0;
        }
    }

    var y: i64 = 1970;
    var total_days: i64 = 0;
    if (year >= 1970) {
        while (y < year) : (y += 1) {
            total_days += if (isLeapYear(y)) 366 else 365;
        }
        total_days += daysBeforeMonth(year, month);
        total_days += (day - 1);
    } else {
        while (y > year) {
            y -= 1;
            total_days -= if (isLeapYear(y)) 366 else 365;
        }
        total_days += daysBeforeMonth(year, month);
        total_days += (day - 1);
    }

    const total_seconds = total_days * 86400 + hour * 3600 + min * 60 + sec;
    return total_seconds * 1000;
}

pub fn mapMariaDbType(type_str: []const u8) DataType {
    var upper_buf: [128]u8 = undefined;
    const len = @min(type_str.len, upper_buf.len);
    const upper = std.ascii.upperString(&upper_buf, type_str[0..len]);

    if (std.mem.startsWith(u8, upper, "TINYINT(1)") or std.mem.startsWith(u8, upper, "BOOL")) {
        return .boolean;
    }
    if (std.mem.startsWith(u8, upper, "INT") or
        std.mem.startsWith(u8, upper, "BIGINT") or
        std.mem.startsWith(u8, upper, "SMALLINT") or
        std.mem.startsWith(u8, upper, "MEDIUMINT") or
        std.mem.startsWith(u8, upper, "TINYINT"))
    {
        return .integer;
    }
    if (std.mem.startsWith(u8, upper, "DECIMAL") or
        std.mem.startsWith(u8, upper, "NUMERIC") or
        std.mem.startsWith(u8, upper, "FLOAT") or
        std.mem.startsWith(u8, upper, "DOUBLE") or
        std.mem.startsWith(u8, upper, "REAL"))
    {
        return .real;
    }
    if (std.mem.startsWith(u8, upper, "TIMESTAMP") or
        std.mem.startsWith(u8, upper, "DATETIME") or
        std.mem.startsWith(u8, upper, "DATE") or
        std.mem.startsWith(u8, upper, "TIME") or
        std.mem.startsWith(u8, upper, "YEAR"))
    {
        return .timestamp;
    }
    if (std.mem.startsWith(u8, upper, "BLOB") or
        std.mem.startsWith(u8, upper, "LONGBLOB") or
        std.mem.startsWith(u8, upper, "MEDIUMBLOB") or
        std.mem.startsWith(u8, upper, "TINYBLOB") or
        std.mem.startsWith(u8, upper, "BINARY") or
        std.mem.startsWith(u8, upper, "VARBINARY"))
    {
        return .blob;
    }
    return .text;
}

pub const MariaDbImporter = struct {
    allocator: std.mem.Allocator,
    engine: *Engine,

    pub fn init(allocator: std.mem.Allocator, engine: *Engine) MariaDbImporter {
        return MariaDbImporter{
            .allocator = allocator,
            .engine = engine,
        };
    }

    pub fn openDumpFile(allocator: std.mem.Allocator, raw_path: []const u8) !std.fs.File {
        var trimmed = std.mem.trim(u8, raw_path, " \t\r\n;");
        // Strip surrounding quotes
        if ((trimmed.len >= 2 and trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\'') or
            (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') or
            (trimmed.len >= 2 and trimmed[0] == '`' and trimmed[trimmed.len - 1] == '`'))
        {
            trimmed = trimmed[1 .. trimmed.len - 1];
        }
        trimmed = std.mem.trim(u8, trimmed, " \t\r\n;");

        // 1. Try directly via cwd
        if (std.fs.cwd().openFile(trimmed, .{})) |f| {
            return f;
        } else |_| {}

        // 2. Try as absolute path if it starts with '/'
        if (std.fs.path.isAbsolute(trimmed)) {
            if (std.fs.openFileAbsolute(trimmed, .{})) |f| {
                return f;
            } else |_| {}
        }

        // 3. If starts with leading slash (e.g. '/dumps/pm_assembly1_14.sql'), strip slash and try relative to cwd
        if (trimmed.len > 1 and trimmed[0] == '/') {
            const no_slash = trimmed[1..];
            if (std.fs.cwd().openFile(no_slash, .{})) |f| {
                return f;
            } else |_| {}
        }

        // 4. Try home directory expansion if starts with '~'
        if (trimmed.len > 0 and trimmed[0] == '~') {
            if (std.posix.getenv("HOME")) |home| {
                const rel_part = if (trimmed.len >= 2 and trimmed[1] == '/') trimmed[2..] else trimmed[1..];
                const expanded = std.fs.path.join(allocator, &.{ home, rel_part }) catch null;
                if (expanded) |exp| {
                    defer allocator.free(exp);
                    if (std.fs.openFileAbsolute(exp, .{})) |f| {
                        return f;
                    } else |_| {}
                }
            }
        }

        // 5. Try inside `./dumps/<basename>`
        const basename = std.fs.path.basename(trimmed);
        const in_dumps = std.fs.path.join(allocator, &.{ "dumps", basename }) catch null;
        if (in_dumps) |idp| {
            defer allocator.free(idp);
            if (std.fs.cwd().openFile(idp, .{})) |f| {
                return f;
            } else |_| {}
        }

        // 6. Try relative to executable directory (e.g. zig-out/bin -> project root)
        var exe_buf: [1024]u8 = undefined;
        if (std.fs.selfExePath(&exe_buf)) |exe_path| {
            if (std.fs.path.dirname(exe_path)) |bin_dir| {
                if (std.fs.path.dirname(bin_dir)) |zig_out_dir| {
                    if (std.fs.path.dirname(zig_out_dir)) |project_dir| {
                        const from_project = std.fs.path.join(allocator, &.{ project_dir, trimmed }) catch null;
                        if (from_project) |fp| {
                            defer allocator.free(fp);
                            if (std.fs.openFileAbsolute(fp, .{})) |f| {
                                return f;
                            } else |_| {}
                        }

                        const from_project_dumps = std.fs.path.join(allocator, &.{ project_dir, "dumps", basename }) catch null;
                        if (from_project_dumps) |fpd| {
                            defer allocator.free(fpd);
                            if (std.fs.openFileAbsolute(fpd, .{})) |f| {
                                return f;
                            } else |_| {}
                        }
                    }
                }
            }
        } else |_| {}

        return error.FileNotFound;
    }

    pub fn importFile(self: *MariaDbImporter, file_path: []const u8) !ImportResult {
        const file = try openDumpFile(self.allocator, file_path);
        defer file.close();

        const stat = try file.stat();
        const content = try self.allocator.alloc(u8, stat.size);
        defer self.allocator.free(content);

        _ = try file.readAll(content);
        return self.importString(content);
    }

    pub fn importString(self: *MariaDbImporter, sql: []const u8) !ImportResult {
        var res = ImportResult{};

        var modified_tables = std.StringHashMap(void).init(self.allocator);
        defer {
            var it = modified_tables.keyIterator();
            while (it.next()) |k| {
                self.allocator.free(k.*);
            }
            modified_tables.deinit();
        }

        var idx: usize = 0;
        var in_single_quote = false;
        var in_double_quote = false;
        var in_backtick = false;
        var in_single_comment = false;
        var in_multi_comment = false;
        var stmt_start: usize = 0;

        while (idx < sql.len) {
            const c = sql[idx];

            if (in_single_comment) {
                if (c == '\n') {
                    in_single_comment = false;
                }
                idx += 1;
                continue;
            }

            if (in_multi_comment) {
                if (c == '*' and idx + 1 < sql.len and sql[idx + 1] == '/') {
                    in_multi_comment = false;
                    idx += 2;
                    continue;
                }
                idx += 1;
                continue;
            }

            if (c == '\\' and (in_single_quote or in_double_quote)) {
                idx += 2;
                continue;
            }

            if (c == '\'' and !in_double_quote and !in_backtick) {
                in_single_quote = !in_single_quote;
                idx += 1;
                continue;
            } else if (c == '"' and !in_single_quote and !in_backtick) {
                in_double_quote = !in_double_quote;
                idx += 1;
                continue;
            } else if (c == '`' and !in_single_quote and !in_double_quote) {
                in_backtick = !in_backtick;
                idx += 1;
                continue;
            }

            if (!in_single_quote and !in_double_quote and !in_backtick) {
                if (c == '#' or (c == '-' and idx + 1 < sql.len and sql[idx + 1] == '-')) {
                    in_single_comment = true;
                    idx += 1;
                    continue;
                }
                if (c == '/' and idx + 1 < sql.len and sql[idx + 1] == '*') {
                    in_multi_comment = true;
                    idx += 2;
                    continue;
                }
                if (c == ';') {
                    const stmt = sql[stmt_start..idx];
                    self.processStatement(stmt, &res, &modified_tables) catch {
                        res.errors += 1;
                    };
                    res.statements_processed += 1;
                    stmt_start = idx + 1;
                    idx += 1;
                    continue;
                }
            }

            idx += 1;
        }

        if (stmt_start < sql.len) {
            const trailing = sql[stmt_start..];
            self.processStatement(trailing, &res, &modified_tables) catch {
                res.errors += 1;
            };
            res.statements_processed += 1;
        }

        // Flush all modified tables to disk and broadcast final chunked snapshot sync to peers
        var tbl_it = modified_tables.keyIterator();
        while (tbl_it.next()) |tbl_k| {
            self.engine.flushTableToDisk(tbl_k.*) catch {};
            self.engine.notifyTableSync(tbl_k.*);
        }

        return res;
    }

    fn stripComments(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
        var out = std.ArrayList(u8).init(allocator);
        errdefer out.deinit();

        var in_sq = false;
        var in_dq = false;
        var i: usize = 0;

        while (i < raw.len) {
            if (raw[i] == '\\' and (in_sq or in_dq)) {
                try out.append(raw[i]);
                if (i + 1 < raw.len) {
                    i += 1;
                    try out.append(raw[i]);
                }
                i += 1;
                continue;
            }

            if (raw[i] == '\'' and !in_dq) {
                in_sq = !in_sq;
                try out.append(raw[i]);
                i += 1;
                continue;
            }

            if (raw[i] == '"' and !in_sq) {
                in_dq = !in_dq;
                try out.append(raw[i]);
                i += 1;
                continue;
            }

            if (!in_sq and !in_dq) {
                if (i + 1 < raw.len and raw[i] == '-' and raw[i + 1] == '-') {
                    while (i < raw.len and raw[i] != '\n') : (i += 1) {}
                    if (i < raw.len) i += 1;
                    continue;
                }
                if (raw[i] == '#') {
                    while (i < raw.len and raw[i] != '\n') : (i += 1) {}
                    if (i < raw.len) i += 1;
                    continue;
                }
                if (i + 1 < raw.len and raw[i] == '/' and raw[i + 1] == '*') {
                    i += 2;
                    while (i + 1 < raw.len and !(raw[i] == '*' and raw[i + 1] == '/')) : (i += 1) {}
                    if (i + 1 < raw.len) i += 2;
                    continue;
                }
            }

            try out.append(raw[i]);
            i += 1;
        }
        return out.toOwnedSlice();
    }

    fn processStatement(
        self: *MariaDbImporter,
        raw_stmt: []const u8,
        res: *ImportResult,
        modified_tables: *std.StringHashMap(void),
    ) !void {
        const clean = try stripComments(self.allocator, raw_stmt);
        defer self.allocator.free(clean);

        const stmt = std.mem.trim(u8, clean, " \t\r\n;");
        if (stmt.len == 0) {
            res.statements_skipped += 1;
            return;
        }

        var lower_buf: [32]u8 = undefined;
        const prefix_len = @min(stmt.len, lower_buf.len);
        const prefix = std.ascii.lowerString(&lower_buf, stmt[0..prefix_len]);

        if (std.mem.startsWith(u8, prefix, "set ") or
            std.mem.startsWith(u8, prefix, "lock tables") or
            std.mem.startsWith(u8, prefix, "unlock tables") or
            std.mem.startsWith(u8, prefix, "use ") or
            std.mem.startsWith(u8, prefix, "/*"))
        {
            res.statements_skipped += 1;
            return;
        }

        if (std.mem.startsWith(u8, prefix, "drop table")) {
            try self.handleDropTable(stmt);
            return;
        }

        if (std.mem.startsWith(u8, prefix, "create table")) {
            try self.handleCreateTable(stmt);
            res.tables_created += 1;
            return;
        }

        if (std.mem.startsWith(u8, prefix, "insert into") or
            std.mem.startsWith(u8, prefix, "insert ignore into") or
            std.mem.startsWith(u8, prefix, "replace into"))
        {
            const count = try self.handleInsert(stmt, modified_tables);
            res.rows_inserted += count;
            return;
        }

        res.statements_skipped += 1;
    }

    fn handleDropTable(self: *MariaDbImporter, stmt: []const u8) !void {
        var it = std.mem.tokenizeAny(u8, stmt, " \t\r\n;");
        _ = it.next(); // DROP
        _ = it.next(); // TABLE
        var tok = it.next() orelse return;
        if (std.ascii.eqlIgnoreCase(tok, "IF")) {
            _ = it.next(); // EXISTS
            tok = it.next() orelse return;
        }
        const tbl_name = unquoteIdentifier(tok);
        _ = try self.engine.dropTable(tbl_name);
    }

    fn handleCreateTable(self: *MariaDbImporter, stmt: []const u8) !void {
        const paren_start = std.mem.indexOfScalar(u8, stmt, '(') orelse return error.InvalidCreateTable;
        const paren_end = std.mem.lastIndexOfScalar(u8, stmt, ')') orelse return error.InvalidCreateTable;
        if (paren_end <= paren_start) return error.InvalidCreateTable;

        const header = stmt[0..paren_start];
        var h_it = std.mem.tokenizeAny(u8, header, " \t\r\n");
        _ = h_it.next(); // CREATE
        _ = h_it.next(); // TABLE
        var tok = h_it.next() orelse return error.InvalidCreateTable;
        if (std.ascii.eqlIgnoreCase(tok, "IF")) {
            _ = h_it.next(); // NOT
            _ = h_it.next(); // EXISTS
            tok = h_it.next() orelse return error.InvalidCreateTable;
        }
        const table_name = unquoteIdentifier(tok);

        const body = stmt[paren_start + 1 .. paren_end];

        var cols = std.ArrayList(Column).init(self.allocator);
        defer cols.deinit();

        var primary_keys = std.StringHashMap(void).init(self.allocator);
        defer primary_keys.deinit();

        // Split body by comma respecting parentheses
        var p_depth: usize = 0;
        var seg_start: usize = 0;
        for (body, 0..) |c, i| {
            if (c == '(') {
                p_depth += 1;
            } else if (c == ')') {
                if (p_depth > 0) p_depth -= 1;
            } else if (c == ',' and p_depth == 0) {
                try self.parseTableItem(body[seg_start..i], &cols, &primary_keys);
                seg_start = i + 1;
            }
        }
        if (seg_start < body.len) {
            try self.parseTableItem(body[seg_start..], &cols, &primary_keys);
        }

        // Apply primary keys from table-level constraints
        for (cols.items) |*c| {
            if (primary_keys.contains(c.name)) {
                c.primary_key = true;
            }
        }

        // If no primary key specified, default first column to primary key
        if (cols.items.len > 0) {
            var has_pk = false;
            for (cols.items) |c| {
                if (c.primary_key) {
                    has_pk = true;
                    break;
                }
            }
            if (!has_pk) {
                cols.items[0].primary_key = true;
            }
        }

        const schema = try Schema.init(self.allocator, cols.items);
        defer schema.deinit(self.allocator);
        _ = try self.engine.createTable(table_name, schema);
    }

    fn parseTableItem(
        self: *MariaDbImporter,
        raw_item: []const u8,
        cols: *std.ArrayList(Column),
        primary_keys: *std.StringHashMap(void),
    ) !void {
        _ = self;
        const item = std.mem.trim(u8, raw_item, " \t\r\n");
        if (item.len == 0) return;

        var upper_buf: [64]u8 = undefined;
        const len = @min(item.len, upper_buf.len);
        const upper = std.ascii.upperString(&upper_buf, item[0..len]);

        if (std.mem.startsWith(u8, upper, "PRIMARY KEY")) {
            if (std.mem.indexOfScalar(u8, item, '(')) |pk_start| {
                if (std.mem.indexOfScalar(u8, item[pk_start..], ')')) |pk_len| {
                    const pk_col_raw = item[pk_start + 1 .. pk_start + pk_len];
                    var pk_it = std.mem.splitScalar(u8, pk_col_raw, ',');
                    while (pk_it.next()) |pk_cand| {
                        const pk_clean = unquoteIdentifier(pk_cand);
                        try primary_keys.put(pk_clean, {});
                    }
                }
            }
            return;
        }

        // Skip secondary indices and foreign keys
        if (std.mem.startsWith(u8, upper, "KEY ") or
            std.mem.startsWith(u8, upper, "UNIQUE KEY") or
            std.mem.startsWith(u8, upper, "CONSTRAINT") or
            std.mem.startsWith(u8, upper, "INDEX") or
            std.mem.startsWith(u8, upper, "FULLTEXT") or
            std.mem.startsWith(u8, upper, "SPATIAL") or
            std.mem.startsWith(u8, upper, "CHECK"))
        {
            return;
        }

        // Column definition: `name` type [attrs]
        var tok_it = std.mem.tokenizeAny(u8, item, " \t\r\n");
        const col_name_raw = tok_it.next() orelse return;
        const col_name = unquoteIdentifier(col_name_raw);
        const col_type_raw = tok_it.next() orelse return;

        const data_type = mapMariaDbType(col_type_raw);
        var is_pk = false;
        var is_nullable = true;
        var is_auto_inc = false;

        while (tok_it.next()) |attr| {
            if (std.ascii.eqlIgnoreCase(attr, "PRIMARY")) {
                if (tok_it.next()) |next_attr| {
                    if (std.ascii.eqlIgnoreCase(next_attr, "KEY")) {
                        is_pk = true;
                    }
                }
            } else if (std.ascii.eqlIgnoreCase(attr, "NOT")) {
                if (tok_it.next()) |next_attr| {
                    if (std.ascii.eqlIgnoreCase(next_attr, "NULL")) {
                        is_nullable = false;
                    }
                }
            } else if (std.ascii.eqlIgnoreCase(attr, "AUTO_INCREMENT") or std.ascii.eqlIgnoreCase(attr, "AUTOINCREMENT")) {
                is_auto_inc = true;
            }
        }

        try cols.append(Column{
            .name = col_name,
            .data_type = data_type,
            .primary_key = is_pk,
            .nullable = is_nullable,
            .auto_increment = is_auto_inc,
        });
    }

    fn handleInsert(
        self: *MariaDbImporter,
        stmt: []const u8,
        modified_tables: *std.StringHashMap(void),
    ) !usize {
        const values_kw_idx = std.ascii.indexOfIgnoreCase(stmt, "VALUES") orelse return error.InvalidInsert;
        const before_values = std.mem.trim(u8, stmt[0..values_kw_idx], " \t\r\n");
        const after_values = std.mem.trim(u8, stmt[values_kw_idx + 6 ..], " \t\r\n");

        var table_name: []const u8 = "";
        var explicit_cols: ?std.ArrayList([]const u8) = null;
        defer if (explicit_cols) |*ec| ec.deinit();

        if (std.mem.indexOfScalar(u8, before_values, '(')) |paren_idx| {
            const table_part = before_values[0..paren_idx];
            var t_it = std.mem.tokenizeAny(u8, table_part, " \t\r\n");
            while (t_it.next()) |tok| {
                if (std.ascii.eqlIgnoreCase(tok, "INSERT") or
                    std.ascii.eqlIgnoreCase(tok, "INTO") or
                    std.ascii.eqlIgnoreCase(tok, "IGNORE") or
                    std.ascii.eqlIgnoreCase(tok, "REPLACE"))
                {
                    continue;
                }
                table_name = unquoteIdentifier(tok);
                break;
            }

            if (std.mem.lastIndexOfScalar(u8, before_values, ')')) |end_idx| {
                if (end_idx > paren_idx) {
                    explicit_cols = std.ArrayList([]const u8).init(self.allocator);
                    const cols_slice = before_values[paren_idx + 1 .. end_idx];
                    var c_it = std.mem.splitScalar(u8, cols_slice, ',');
                    while (c_it.next()) |c_cand| {
                        try explicit_cols.?.append(unquoteIdentifier(c_cand));
                    }
                }
            }
        } else {
            var t_it = std.mem.tokenizeAny(u8, before_values, " \t\r\n");
            while (t_it.next()) |tok| {
                if (std.ascii.eqlIgnoreCase(tok, "INSERT") or
                    std.ascii.eqlIgnoreCase(tok, "INTO") or
                    std.ascii.eqlIgnoreCase(tok, "IGNORE") or
                    std.ascii.eqlIgnoreCase(tok, "REPLACE"))
                {
                    continue;
                }
                table_name = unquoteIdentifier(tok);
                break;
            }
        }

        if (table_name.len == 0) return error.InvalidInsert;

        if (!modified_tables.contains(table_name)) {
            const dup_tbl = try self.allocator.dupe(u8, table_name);
            try modified_tables.put(dup_tbl, {});
        }

        const table = self.engine.getTable(table_name) orelse return error.TableNotFound;

        // Parse tuples in after_values: `(1, 'a'), (2, 'b')`
        var inserted_count: usize = 0;
        var in_single_quote = false;
        var in_double_quote = false;
        var tuple_depth: usize = 0;
        var tuple_start: usize = 0;

        var i: usize = 0;
        while (i < after_values.len) {
            const c = after_values[i];
            if (c == '\\' and (in_single_quote or in_double_quote)) {
                i += 2;
                continue;
            }
            if (c == '\'' and in_single_quote and i + 1 < after_values.len and after_values[i + 1] == '\'') {
                i += 2;
                continue;
            }
            if (c == '\'' and !in_double_quote) {
                in_single_quote = !in_single_quote;
            } else if (c == '"' and !in_single_quote) {
                in_double_quote = !in_double_quote;
            } else if (!in_single_quote and !in_double_quote) {
                if (c == '(') {
                    if (tuple_depth == 0) tuple_start = i + 1;
                    tuple_depth += 1;
                } else if (c == ')') {
                    if (tuple_depth == 1) {
                        const tuple_str = after_values[tuple_start..i];
                        try self.insertTuple(table, table_name, table.schema, explicit_cols, tuple_str);
                        inserted_count += 1;
                    }
                    if (tuple_depth > 0) tuple_depth -= 1;
                }
            }
            i += 1;
        }

        return inserted_count;
    }

    fn insertTuple(
        self: *MariaDbImporter,
        table: *Table,
        table_name: []const u8,
        schema: Schema,
        explicit_cols: ?std.ArrayList([]const u8),
        tuple_str: []const u8,
    ) !void {
        var raw_vals_buf: [128][]const u8 = undefined;
        var raw_vals_count: usize = 0;

        var in_sq = false;
        var in_dq = false;
        var val_start: usize = 0;

        var i: usize = 0;
        while (i < tuple_str.len) {
            const c = tuple_str[i];
            if (c == '\\' and (in_sq or in_dq)) {
                i += 2;
                continue;
            }
            if (c == '\'' and !in_dq) {
                in_sq = !in_sq;
            } else if (c == '"' and !in_sq) {
                in_dq = !in_dq;
            } else if (c == ',' and !in_sq and !in_dq) {
                if (raw_vals_count < raw_vals_buf.len) {
                    raw_vals_buf[raw_vals_count] = std.mem.trim(u8, tuple_str[val_start..i], " \t\r\n");
                    raw_vals_count += 1;
                }
                val_start = i + 1;
            }
            i += 1;
        }
        if (val_start < tuple_str.len) {
            if (raw_vals_count < raw_vals_buf.len) {
                raw_vals_buf[raw_vals_count] = std.mem.trim(u8, tuple_str[val_start..], " \t\r\n");
                raw_vals_count += 1;
            }
        }
        const raw_vals = raw_vals_buf[0..raw_vals_count];

        var full_row = try Row.init(self.allocator, schema.columns.len);
        errdefer full_row.deinit(self.allocator);

        if (explicit_cols) |cols| {
            if (cols.items.len != raw_vals.len) return error.ColumnCountMismatch;
            for (cols.items, 0..) |col_name, c_idx| {
                const schema_idx = schema.getColumnIndex(col_name) orelse return error.ColumnNotFound;
                const col_type = schema.columns[schema_idx].data_type;
                full_row.values[schema_idx] = try self.parseValue(raw_vals[c_idx], col_type);
            }
        } else {
            if (raw_vals.len != schema.columns.len) return error.ColumnCountMismatch;
            for (schema.columns, 0..) |col, c_idx| {
                full_row.values[c_idx] = try self.parseValue(raw_vals[c_idx], col.data_type);
            }
        }

        // Auto-increment primary key if unassigned or null
        if (schema.getPrimaryKeyIndex()) |pk_idx| {
            if (full_row.values[pk_idx] == .null_val) {
                full_row.values[pk_idx] = Value{ .integer = table.next_auto_inc };
                table.next_auto_inc += 1;
            } else if (full_row.values[pk_idx] == .integer) {
                if (full_row.values[pk_idx].integer >= table.next_auto_inc) {
                    table.next_auto_inc = full_row.values[pk_idx].integer + 1;
                }
            }
        }

        try self.engine.insertRowBulkDirect(table_name, full_row);
    }

    fn parseValue(self: *MariaDbImporter, raw: []const u8, target_type: DataType) !Value {
        const trimmed = std.mem.trim(u8, raw, " \t\r\n");

        if (std.ascii.eqlIgnoreCase(trimmed, "NULL") or std.mem.eql(u8, trimmed, "\\N")) {
            return Value{ .null_val = {} };
        }

        // Quoted string
        if ((trimmed.len >= 2 and trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\'') or
            (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"'))
        {
            const inner = trimmed[1 .. trimmed.len - 1];
            const unescaped = try unescapeSqlString(self.allocator, inner);

            switch (target_type) {
                .text => return Value{ .text = unescaped },
                .timestamp => {
                    if (parseSqlDateTimeToMillis(unescaped)) |ts| {
                        self.allocator.free(unescaped);
                        return Value{ .timestamp = ts };
                    }
                    if (std.fmt.parseInt(i64, unescaped, 10) catch null) |ts_int| {
                        self.allocator.free(unescaped);
                        return Value{ .timestamp = ts_int };
                    }
                    self.allocator.free(unescaped);
                    return Value{ .timestamp = 0 };
                },
                .real => {
                    const r = std.fmt.parseFloat(f64, unescaped) catch 0.0;
                    self.allocator.free(unescaped);
                    return Value{ .real = r };
                },
                .integer => {
                    const int_val = std.fmt.parseInt(i64, unescaped, 10) catch 0;
                    self.allocator.free(unescaped);
                    return Value{ .integer = int_val };
                },
                .boolean => {
                    const b = std.ascii.eqlIgnoreCase(unescaped, "true") or std.mem.eql(u8, unescaped, "1");
                    self.allocator.free(unescaped);
                    return Value{ .boolean = b };
                },
                .blob => return Value{ .blob = unescaped },
            }
        }

        // Unquoted literals (numbers, booleans)
        if (std.ascii.eqlIgnoreCase(trimmed, "true")) {
            return Value{ .boolean = true };
        } else if (std.ascii.eqlIgnoreCase(trimmed, "false")) {
            return Value{ .boolean = false };
        }

        switch (target_type) {
            .boolean => {
                if (std.fmt.parseInt(i64, trimmed, 10)) |iv| {
                    return Value{ .boolean = (iv != 0) };
                } else |_| {
                    return Value{ .boolean = false };
                }
            },
            .integer => {
                const iv = std.fmt.parseInt(i64, trimmed, 10) catch 0;
                return Value{ .integer = iv };
            },
            .real => {
                const fv = std.fmt.parseFloat(f64, trimmed) catch 0.0;
                return Value{ .real = fv };
            },
            .timestamp => {
                const ts = std.fmt.parseInt(i64, trimmed, 10) catch 0;
                return Value{ .timestamp = ts };
            },
            .blob => {
                return Value{ .blob = try self.allocator.dupe(u8, trimmed) };
            },
            .text => {
                return Value{ .text = try self.allocator.dupe(u8, trimmed) };
            },
        }
    }
};

test "MariaDB 11.3 Dump File Compatibility and Ingestion" {
    const allocator = std.testing.allocator;

    const tmp_dir = "./test_data_mariadb";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const engine = try Engine.init(allocator, tmp_dir, "MariaDB-Node");
    defer engine.deinit();

    var importer = MariaDbImporter.init(allocator, engine);

    const mariadb_dump_sql =
        \\-- MariaDB dump 10.19  Distrib 10.11.3-MariaDB, for debian-linux-gnu (x86_64)
        \\--
        \\-- Host: localhost    Database: enterprise_erp
        \\-- ------------------------------------------------------
        \\-- Server version	11.3.2-MariaDB
        \\
        \\/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
        \\/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
        \\/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
        \\/*!40101 SET NAMES utf8mb4 */;
        \\/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
        \\/*!40103 SET TIME_ZONE='+00:00' */;
        \\/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
        \\/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
        \\/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
        \\/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
        \\
        \\--
        \\-- Table structure for table `customers`
        \\--
        \\
        \\DROP TABLE IF EXISTS `customers`;
        \\/*!40101 SET @saved_cs_client     = @@character_set_client */;
        \\/*!40101 SET character_set_client = utf8 */;
        \\CREATE TABLE `customers` (
        \\  `id` int(11) NOT NULL AUTO_INCREMENT,
        \\  `name` varchar(100) NOT NULL,
        \\  `email` varchar(150) DEFAULT NULL,
        \\  `balance` decimal(10,2) NOT NULL DEFAULT 0.00,
        \\  `is_active` tinyint(1) DEFAULT 1,
        \\  `created_at` datetime DEFAULT current_timestamp(),
        \\  PRIMARY KEY (`id`),
        \\  KEY `idx_email` (`email`)
        \\) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
        \\/*!40101 SET character_set_client = @saved_cs_client */;
        \\
        \\--
        \\-- Dumping data for table `customers`
        \\--
        \\
        \\LOCK TABLES `customers` WRITE;
        \\/*!40000 ALTER TABLE `customers` DISABLE KEYS */;
        \\INSERT INTO `customers` (`id`, `name`, `email`, `balance`, `is_active`, `created_at`) VALUES
        \\(1,'Alice Johnson','alice@corp.com',1250.50,1,'2024-01-15 09:30:00'),
        \\(2,'Bob O\'Connor','bob@example.com',-45.20,0,'2024-02-01 14:15:22'),
        \\(3,'Charlie \"Chuck\" Smith',NULL,0.00,1,'2024-03-10 11:00:00');
        \\/*!40000 ALTER TABLE `customers` ENABLE KEYS */;
        \\UNLOCK TABLES;
    ;

    const result = try importer.importString(mariadb_dump_sql);
    try std.testing.expectEqual(@as(usize, 1), result.tables_created);
    try std.testing.expectEqual(@as(usize, 3), result.rows_inserted);
    try std.testing.expectEqual(@as(usize, 0), result.errors);

    // Verify Table Schema
    const table = engine.getTable("customers") orelse return error.TestExpectedTable;
    try std.testing.expectEqual(@as(usize, 6), table.schema.columns.len);
    try std.testing.expectEqualStrings("id", table.schema.columns[0].name);
    try std.testing.expect(table.schema.columns[0].primary_key);
    try std.testing.expectEqual(DataType.integer, table.schema.columns[0].data_type);

    try std.testing.expectEqualStrings("name", table.schema.columns[1].name);
    try std.testing.expectEqual(DataType.text, table.schema.columns[1].data_type);

    try std.testing.expectEqualStrings("email", table.schema.columns[2].name);
    try std.testing.expect(table.schema.columns[2].nullable);

    try std.testing.expectEqualStrings("balance", table.schema.columns[3].name);
    try std.testing.expectEqual(DataType.real, table.schema.columns[3].data_type);

    try std.testing.expectEqualStrings("is_active", table.schema.columns[4].name);
    try std.testing.expectEqual(DataType.boolean, table.schema.columns[4].data_type);

    try std.testing.expectEqualStrings("created_at", table.schema.columns[5].name);
    try std.testing.expectEqual(DataType.timestamp, table.schema.columns[5].data_type);

    // Verify Row 1: Alice Johnson
    const row1 = (try table.getRowByPk(Value{ .integer = 1 })) orelse return error.TestExpectedRow;
    defer row1.deinit(allocator);
    try std.testing.expectEqualStrings("Alice Johnson", row1.values[1].text);
    try std.testing.expectEqualStrings("alice@corp.com", row1.values[2].text);
    try std.testing.expectEqual(@as(f64, 1250.50), row1.values[3].real);
    try std.testing.expect(row1.values[4].boolean);

    // Verify Row 2: Bob O'Connor (escaped single quote)
    const row2 = (try table.getRowByPk(Value{ .integer = 2 })) orelse return error.TestExpectedRow;
    defer row2.deinit(allocator);
    try std.testing.expectEqualStrings("Bob O'Connor", row2.values[1].text);
    try std.testing.expectEqualStrings("bob@example.com", row2.values[2].text);
    try std.testing.expectEqual(@as(f64, -45.20), row2.values[3].real);
    try std.testing.expect(!row2.values[4].boolean);

    // Verify Row 3: Charlie "Chuck" Smith with NULL email
    const row3 = (try table.getRowByPk(Value{ .integer = 3 })) orelse return error.TestExpectedRow;
    defer row3.deinit(allocator);
    try std.testing.expectEqualStrings("Charlie \"Chuck\" Smith", row3.values[1].text);
    try std.testing.expect(row3.values[2] == .null_val);
    try std.testing.expectEqual(@as(f64, 0.0), row3.values[3].real);
    try std.testing.expect(row3.values[4].boolean);
}

test "MariaDB 11.3 Multi-Table and Auto-Increment PK Generation" {
    const allocator = std.testing.allocator;

    const tmp_dir = "./test_data_mariadb_multi";
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    const engine = try Engine.init(allocator, tmp_dir, "MultiTable-Node");
    defer engine.deinit();

    var importer = MariaDbImporter.init(allocator, engine);

    const sql =
        \\CREATE TABLE `products` (
        \\  `product_id` int(11) NOT NULL AUTO_INCREMENT,
        \\  `sku` varchar(50) NOT NULL,
        \\  `price` decimal(10,2) NOT NULL,
        \\  `in_stock` tinyint(1) NOT NULL DEFAULT 1,
        \\  PRIMARY KEY (`product_id`)
        \\) ENGINE=InnoDB;
        \\
        \\CREATE TABLE `orders` (
        \\  `order_id` int(11) NOT NULL AUTO_INCREMENT,
        \\  `customer_name` varchar(100) NOT NULL,
        \\  `order_date` date NOT NULL,
        \\  `total_amount` double NOT NULL,
        \\  PRIMARY KEY (`order_id`),
        \\  CONSTRAINT `fk_orders_customer` FOREIGN KEY (`customer_name`) REFERENCES `customers` (`name`)
        \\) ENGINE=InnoDB;
        \\
        \\INSERT INTO `products` (`product_id`, `sku`, `price`, `in_stock`) VALUES
        \\(101, 'LAPTOP-PRO-16', 1999.99, 1),
        \\(102, 'WIRELESS-MOUSE', 49.50, 1);
        \\
        \\-- Insert orders with NULL order_id testing automatic autoincrement ID assignment
        \\INSERT INTO `orders` (`order_id`, `customer_name`, `order_date`, `total_amount`) VALUES
        \\(NULL, 'Alice Johnson', '2024-05-20', 2049.49),
        \\(NULL, 'Bob O\'Connor', '2024-06-01', 49.50);
    ;

    const res = try importer.importString(sql);
    try std.testing.expectEqual(@as(usize, 2), res.tables_created);
    try std.testing.expectEqual(@as(usize, 4), res.rows_inserted);
    try std.testing.expectEqual(@as(usize, 0), res.errors);

    const products = engine.getTable("products") orelse return error.MissingProducts;
    try std.testing.expectEqual(@as(usize, 2), products.rows.items.len);

    const orders = engine.getTable("orders") orelse return error.MissingOrders;
    try std.testing.expectEqual(@as(usize, 2), orders.rows.items.len);

    // Verify orders got auto-assigned PKs 1 and 2
    const ord1 = (try orders.getRowByPk(Value{ .integer = 1 })) orelse return error.MissingOrder1;
    defer ord1.deinit(allocator);
    try std.testing.expectEqualStrings("Alice Johnson", ord1.values[1].text);

    const ord2 = (try orders.getRowByPk(Value{ .integer = 2 })) orelse return error.MissingOrder2;
    defer ord2.deinit(allocator);
    try std.testing.expectEqualStrings("Bob O'Connor", ord2.values[1].text);
}

