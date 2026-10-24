const std = @import("std");
const types = @import("../types.zig");
const DataType = types.DataType;
const Column = types.Column;
const Value = types.Value;
const token = @import("token.zig");
const Token = token.Token;
const TokenType = token.TokenType;
const Lexer = token.Lexer;
const ast = @import("ast.zig");
const Statement = ast.Statement;
const SelectItem = ast.SelectItem;
const AggregateType = ast.AggregateType;
const Expr = ast.Expr;
const BinaryOp = ast.BinaryOp;
const JoinClause = ast.JoinClause;
const OrderBy = ast.OrderBy;
const Assignment = ast.Assignment;

pub const Parser = struct {
    allocator: std.mem.Allocator,
    tokens: []Token,
    cursor: usize = 0,

    pub fn init(allocator: std.mem.Allocator, source: []const u8) !Parser {
        var lexer = Lexer.init(source);
        var token_list = std.ArrayList(Token).init(allocator);
        errdefer token_list.deinit();

        while (true) {
            const tok = lexer.nextToken();
            try token_list.append(tok);
            if (tok.token_type == .eof) break;
        }

        return Parser{
            .allocator = allocator,
            .tokens = token_list.toOwnedSlice() catch unreachable,
            .cursor = 0,
        };
    }

    pub fn deinit(self: *Parser) void {
        self.allocator.free(self.tokens);
    }

    fn current(self: *Parser) Token {
        if (self.cursor >= self.tokens.len) {
            return self.tokens[self.tokens.len - 1];
        }
        return self.tokens[self.cursor];
    }

    fn check(self: *Parser, ttype: TokenType) bool {
        return self.current().token_type == ttype;
    }

    fn advance(self: *Parser) Token {
        const tok = self.current();
        if (tok.token_type != .eof) {
            self.cursor += 1;
        }
        return tok;
    }

    fn match(self: *Parser, ttype: TokenType) bool {
        if (self.check(ttype)) {
            _ = self.advance();
            return true;
        }
        return false;
    }

    fn expect(self: *Parser, ttype: TokenType) !Token {
        if (self.check(ttype)) {
            return self.advance();
        }
        return error.UnexpectedToken;
    }

    pub fn parse(self: *Parser) !Statement {
        const tok = self.current();
        switch (tok.token_type) {
            .kw_create => return try self.parseCreate(),
            .kw_alter => return try self.parseAlter(),
            .kw_drop => return try self.parseDrop(),
            .kw_insert => return try self.parseInsert(),
            .kw_select => return try self.parseSelect(),
            .kw_update => return try self.parseUpdate(),
            .kw_delete => return try self.parseDelete(),
            .kw_show => return try self.parseShow(),
            .kw_describe => return try self.parseDescribe(),
            .kw_peers => {
                _ = self.advance();
                _ = self.match(.semicolon);
                return Statement{ .peers = {} };
            },
            .kw_sync => return try self.parseSync(),
            .kw_status => {
                _ = self.advance();
                _ = self.match(.semicolon);
                return Statement{ .status = {} };
            },
            .kw_help => {
                _ = self.advance();
                _ = self.match(.semicolon);
                return Statement{ .help = {} };
            },
            .kw_exit, .kw_quit => {
                _ = self.advance();
                _ = self.match(.semicolon);
                return Statement{ .exit = {} };
            },
            else => return error.InvalidStatement,
        }
    }

    fn parseAlter(self: *Parser) !Statement {
        _ = try self.expect(.kw_alter);
        _ = try self.expect(.kw_table);
        const table_name_tok = try self.expect(.identifier);

        if (self.match(.kw_add)) {
            if (self.match(.kw_primary)) {
                _ = try self.expect(.kw_key);
                const has_paren = self.match(.lparen);
                const pk_col_tok = try self.expect(.identifier);
                if (has_paren) _ = try self.expect(.rparen);
                _ = self.match(.semicolon);
                return Statement{
                    .alter_table = .{
                        .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                        .action = .{
                            .add_primary_key = try self.allocator.dupe(u8, pk_col_tok.lexeme),
                        },
                    },
                };
            }
            _ = self.match(.kw_column);
            const col_name_tok = try self.expect(.identifier);
            const type_tok = self.advance();
            const data_type = DataType.fromString(type_tok.lexeme) orelse return error.UnknownDataType;

            var is_pk = false;
            var is_auto_inc = false;
            var nullable = true;

            while (true) {
                if (self.match(.kw_primary)) {
                    _ = try self.expect(.kw_key);
                    is_pk = true;
                    nullable = false;
                } else if (self.match(.kw_autoincrement)) {
                    is_auto_inc = true;
                } else if (self.match(.kw_not)) {
                    _ = try self.expect(.kw_null);
                    nullable = false;
                } else if (self.match(.kw_null)) {
                    nullable = true;
                } else {
                    break;
                }
            }

            _ = self.match(.semicolon);
            return Statement{
                .alter_table = .{
                    .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                    .action = .{
                        .add_column = .{
                            .name = try self.allocator.dupe(u8, col_name_tok.lexeme),
                            .data_type = data_type,
                            .primary_key = is_pk,
                            .nullable = if (is_pk) false else nullable,
                            .auto_increment = is_auto_inc,
                        },
                    },
                },
            };
        } else if (self.match(.kw_drop)) {
            if (self.match(.kw_primary)) {
                _ = try self.expect(.kw_key);
                _ = self.match(.semicolon);
                return Statement{
                    .alter_table = .{
                        .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                        .action = .{
                            .drop_primary_key = {},
                        },
                    },
                };
            }
            _ = self.match(.kw_column);
            const col_name_tok = try self.expect(.identifier);
            _ = self.match(.semicolon);
            return Statement{
                .alter_table = .{
                    .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                    .action = .{
                        .drop_column = try self.allocator.dupe(u8, col_name_tok.lexeme),
                    },
                },
            };
        } else if (self.match(.kw_modify)) {
            _ = self.match(.kw_column);
            const col_name_tok = try self.expect(.identifier);
            const type_tok = self.advance();
            const data_type = DataType.fromString(type_tok.lexeme) orelse return error.UnknownDataType;

            var is_pk = false;
            var is_auto_inc = false;
            var nullable = true;

            while (true) {
                if (self.match(.kw_primary)) {
                    _ = try self.expect(.kw_key);
                    is_pk = true;
                    nullable = false;
                } else if (self.match(.kw_autoincrement)) {
                    is_auto_inc = true;
                } else if (self.match(.kw_not)) {
                    _ = try self.expect(.kw_null);
                    nullable = false;
                } else if (self.match(.kw_null)) {
                    nullable = true;
                } else {
                    break;
                }
            }

            _ = self.match(.semicolon);
            return Statement{
                .alter_table = .{
                    .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                    .action = .{
                        .modify_column = .{
                            .name = try self.allocator.dupe(u8, col_name_tok.lexeme),
                            .data_type = data_type,
                            .primary_key = is_pk,
                            .nullable = if (is_pk) false else nullable,
                            .auto_increment = is_auto_inc,
                        },
                    },
                },
            };
        } else if (self.match(.kw_rename)) {
            _ = self.match(.kw_column);
            const old_name_tok = try self.expect(.identifier);
            _ = try self.expect(.kw_to);
            const new_name_tok = try self.expect(.identifier);
            _ = self.match(.semicolon);
            return Statement{
                .alter_table = .{
                    .table_name = try self.allocator.dupe(u8, table_name_tok.lexeme),
                    .action = .{
                        .rename_column = .{
                            .old_name = try self.allocator.dupe(u8, old_name_tok.lexeme),
                            .new_name = try self.allocator.dupe(u8, new_name_tok.lexeme),
                        },
                    },
                },
            };
        } else {
            return error.InvalidAlterAction;
        }
    }

    fn parseCreate(self: *Parser) !Statement {
        _ = try self.expect(.kw_create);
        _ = try self.expect(.kw_table);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.lparen);

        var cols = std.ArrayList(Column).init(self.allocator);
        errdefer cols.deinit();

        while (true) {
            const col_name_tok = try self.expect(.identifier);
            const type_tok = self.advance();
            const data_type = DataType.fromString(type_tok.lexeme) orelse return error.UnknownDataType;

            var is_pk = false;
            var is_auto_inc = false;
            var is_nullable = true;

            while (true) {
                if (self.match(.kw_primary)) {
                    _ = try self.expect(.kw_key);
                    is_pk = true;
                    is_nullable = false;
                } else if (self.match(.kw_autoincrement)) {
                    is_auto_inc = true;
                } else if (self.match(.kw_not)) {
                    _ = try self.expect(.kw_null);
                    is_nullable = false;
                } else if (self.match(.kw_null)) {
                    is_nullable = true;
                } else {
                    break;
                }
            }

            const col = Column{
                .name = try self.allocator.dupe(u8, col_name_tok.lexeme),
                .data_type = data_type,
                .primary_key = is_pk,
                .nullable = if (is_pk) false else is_nullable,
                .auto_increment = is_auto_inc,
            };
            try cols.append(col);

            if (self.match(.comma)) {
                continue;
            } else if (self.match(.rparen)) {
                break;
            } else {
                return error.ExpectedCommaOrRparen;
            }
        }
        _ = self.match(.semicolon);

        return Statement{
            .create_table = .{
                .table_name = try self.allocator.dupe(u8, name_tok.lexeme),
                .columns = try cols.toOwnedSlice(),
            },
        };
    }

    fn parseDrop(self: *Parser) !Statement {
        _ = try self.expect(.kw_drop);
        _ = try self.expect(.kw_table);
        const name_tok = try self.expect(.identifier);
        _ = self.match(.semicolon);
        return Statement{
            .drop_table = .{
                .table_name = try self.allocator.dupe(u8, name_tok.lexeme),
            },
        };
    }

    fn parseInsert(self: *Parser) !Statement {
        _ = try self.expect(.kw_insert);
        _ = try self.expect(.kw_into);
        const name_tok = try self.expect(.identifier);

        var specified_cols: ?[][]const u8 = null;
        if (self.match(.lparen)) {
            var col_names = std.ArrayList([]const u8).init(self.allocator);
            while (true) {
                const ctok = try self.expect(.identifier);
                try col_names.append(try self.allocator.dupe(u8, ctok.lexeme));
                if (self.match(.comma)) continue;
                if (self.match(.rparen)) break;
                return error.ExpectedCommaOrRparen;
            }
            specified_cols = try col_names.toOwnedSlice();
        }

        _ = try self.expect(.kw_values);
        _ = try self.expect(.lparen);

        var vals = std.ArrayList(Value).init(self.allocator);
        errdefer vals.deinit();

        while (true) {
            const v = try self.parseLiteralValue();
            try vals.append(v);
            if (self.match(.comma)) continue;
            if (self.match(.rparen)) break;
            return error.ExpectedCommaOrRparen;
        }
        _ = self.match(.semicolon);

        return Statement{
            .insert = .{
                .table_name = try self.allocator.dupe(u8, name_tok.lexeme),
                .columns = specified_cols,
                .values = try vals.toOwnedSlice(),
            },
        };
    }

    fn parseLiteralValue(self: *Parser) !Value {
        const tok = self.advance();
        switch (tok.token_type) {
            .number_literal => {
                if (std.mem.indexOfScalar(u8, tok.lexeme, '.') != null) {
                    const r = try std.fmt.parseFloat(f64, tok.lexeme);
                    return Value{ .real = r };
                } else {
                    const i = try std.fmt.parseInt(i64, tok.lexeme, 10);
                    return Value{ .integer = i };
                }
            },
            .string_literal => {
                return Value{ .text = try self.allocator.dupe(u8, tok.lexeme) };
            },
            .kw_true => return Value{ .boolean = true },
            .kw_false => return Value{ .boolean = false },
            .kw_null => return Value{ .null_val = {} },
            else => return error.InvalidLiteral,
        }
    }

    fn parseSelect(self: *Parser) !Statement {
        _ = try self.expect(.kw_select);
        var items = std.ArrayList(SelectItem).init(self.allocator);
        errdefer items.deinit();

        while (true) {
            if (self.match(.asterisk)) {
                try items.append(.{ .star = true });
            } else {
                const id_tok = try self.expect(.identifier);
                if (self.check(.lparen)) {
                    // Aggregate function
                    _ = self.advance(); // '('
                    const func_name = id_tok.lexeme;
                    var agg_type: AggregateType = .count;
                    if (std.ascii.eqlIgnoreCase(func_name, "COUNT")) agg_type = .count
                    else if (std.ascii.eqlIgnoreCase(func_name, "SUM")) agg_type = .sum
                    else if (std.ascii.eqlIgnoreCase(func_name, "AVG")) agg_type = .avg
                    else if (std.ascii.eqlIgnoreCase(func_name, "MIN")) agg_type = .min
                    else if (std.ascii.eqlIgnoreCase(func_name, "MAX")) agg_type = .max
                    else return error.UnknownAggregate;

                    var agg_col: ?[]const u8 = null;
                    if (self.match(.asterisk)) {
                        agg_col = try self.allocator.dupe(u8, "*");
                    } else {
                        const c = try self.expect(.identifier);
                        agg_col = try self.allocator.dupe(u8, c.lexeme);
                    }
                    _ = try self.expect(.rparen);

                    try items.append(.{
                        .aggregate = agg_type,
                        .column_name = agg_col,
                    });
                } else if (self.match(.dot)) {
                    const col_tok = try self.expect(.identifier);
                    try items.append(.{
                        .table_name = try self.allocator.dupe(u8, id_tok.lexeme),
                        .column_name = try self.allocator.dupe(u8, col_tok.lexeme),
                    });
                } else {
                    try items.append(.{
                        .column_name = try self.allocator.dupe(u8, id_tok.lexeme),
                    });
                }
            }

            if (self.match(.comma)) continue;
            break;
        }

        _ = try self.expect(.kw_from);
        const from_tok = try self.expect(.identifier);

        var join_clause: ?JoinClause = null;
        if (self.match(.kw_join)) {
            const jtable = try self.expect(.identifier);
            _ = try self.expect(.kw_on);
            const ltok = try self.expect(.identifier);
            _ = try self.expect(.dot);
            const lcol = try self.expect(.identifier);
            _ = try self.expect(.equal);
            const rtok = try self.expect(.identifier);
            _ = try self.expect(.dot);
            const rcol = try self.expect(.identifier);

            join_clause = JoinClause{
                .join_table = try self.allocator.dupe(u8, jtable.lexeme),
                .left_col = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ ltok.lexeme, lcol.lexeme }),
                .right_col = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ rtok.lexeme, rcol.lexeme }),
            };
        }

        var where_expr: ?*Expr = null;
        if (self.match(.kw_where)) {
            where_expr = try self.parseExpression();
        }

        var order_by: ?OrderBy = null;
        if (self.match(.kw_order)) {
            _ = try self.expect(.kw_by);
            const ord_col = try self.expect(.identifier);
            var asc = true;
            if (self.match(.kw_desc)) {
                asc = false;
            } else {
                _ = self.match(.kw_asc);
            }
            order_by = OrderBy{
                .column_name = try self.allocator.dupe(u8, ord_col.lexeme),
                .ascending = asc,
            };
        }

        var limit_val: ?usize = null;
        if (self.match(.kw_limit)) {
            const lim_tok = try self.expect(.number_literal);
            limit_val = try std.fmt.parseInt(usize, lim_tok.lexeme, 10);
        }

        _ = self.match(.semicolon);

        return Statement{
            .select = .{
                .items = try items.toOwnedSlice(),
                .from_table = try self.allocator.dupe(u8, from_tok.lexeme),
                .join = join_clause,
                .where = where_expr,
                .order_by = order_by,
                .limit = limit_val,
            },
        };
    }

    fn parseUpdate(self: *Parser) !Statement {
        _ = try self.expect(.kw_update);
        const name_tok = try self.expect(.identifier);
        _ = try self.expect(.kw_set);

        var assigns = std.ArrayList(Assignment).init(self.allocator);
        errdefer assigns.deinit();

        while (true) {
            const col_tok = try self.expect(.identifier);
            _ = try self.expect(.equal);
            const val = try self.parseLiteralValue();
            try assigns.append(.{
                .column_name = try self.allocator.dupe(u8, col_tok.lexeme),
                .value = val,
            });
            if (self.match(.comma)) continue;
            break;
        }

        var where_expr: ?*Expr = null;
        if (self.match(.kw_where)) {
            where_expr = try self.parseExpression();
        }
        _ = self.match(.semicolon);

        return Statement{
            .update = .{
                .table_name = try self.allocator.dupe(u8, name_tok.lexeme),
                .assignments = try assigns.toOwnedSlice(),
                .where = where_expr,
            },
        };
    }

    fn parseDelete(self: *Parser) !Statement {
        _ = try self.expect(.kw_delete);
        _ = try self.expect(.kw_from);
        const name_tok = try self.expect(.identifier);

        var where_expr: ?*Expr = null;
        if (self.match(.kw_where)) {
            where_expr = try self.parseExpression();
        }
        _ = self.match(.semicolon);

        return Statement{
            .delete = .{
                .table_name = try self.allocator.dupe(u8, name_tok.lexeme),
                .where = where_expr,
            },
        };
    }

    fn parseShow(self: *Parser) !Statement {
        _ = try self.expect(.kw_show);
        _ = try self.expect(.kw_tables);
        _ = self.match(.semicolon);
        return Statement{ .show_tables = {} };
    }

    fn parseDescribe(self: *Parser) !Statement {
        _ = try self.expect(.kw_describe);
        const name_tok = try self.expect(.identifier);
        _ = self.match(.semicolon);
        return Statement{
            .describe = try self.allocator.dupe(u8, name_tok.lexeme),
        };
    }

    fn parseSync(self: *Parser) !Statement {
        _ = try self.expect(.kw_sync);
        const addr_tok = self.advance();
        _ = self.match(.semicolon);
        return Statement{
            .sync_connect = .{
                .address = try self.allocator.dupe(u8, addr_tok.lexeme),
            },
        };
    }

    fn parseExpression(self: *Parser) !*Expr {
        var left = try self.parseComparison();

        while (self.check(.kw_and) or self.check(.kw_or)) {
            const op_tok = self.advance();
            const op: BinaryOp = if (op_tok.token_type == .kw_and) .and_op else .or_op;
            const right = try self.parseComparison();

            const node = try self.allocator.create(Expr);
            node.* = .{
                .binary = .{
                    .left = left,
                    .op = op,
                    .right = right,
                },
            };
            left = node;
        }

        return left;
    }

    fn parseComparison(self: *Parser) !*Expr {
        const left = try self.parsePrimaryExpr();

        var op: ?BinaryOp = null;
        if (self.match(.equal)) op = .eq
        else if (self.match(.not_equal)) op = .neq
        else if (self.match(.less)) op = .lt
        else if (self.match(.less_equal)) op = .lte
        else if (self.match(.greater)) op = .gt
        else if (self.match(.greater_equal)) op = .gte;

        if (op) |binary_op| {
            const right = try self.parsePrimaryExpr();
            const node = try self.allocator.create(Expr);
            node.* = .{
                .binary = .{
                    .left = left,
                    .op = binary_op,
                    .right = right,
                },
            };
            return node;
        }

        return left;
    }

    fn parsePrimaryExpr(self: *Parser) !*Expr {
        const node = try self.allocator.create(Expr);
        errdefer self.allocator.destroy(node);

        const tok = self.current();
        if (tok.token_type == .identifier) {
            _ = self.advance();
            if (self.match(.dot)) {
                const ctok = try self.expect(.identifier);
                node.* = .{
                    .column = .{
                        .table_name = tok.lexeme,
                        .column_name = ctok.lexeme,
                    },
                };
            } else {
                node.* = .{
                    .column = .{
                        .column_name = tok.lexeme,
                    },
                };
            }
            return node;
        } else {
            const lit_val = try self.parseLiteralValue();
            node.* = .{ .literal = lit_val };
            return node;
        }
    }
};
