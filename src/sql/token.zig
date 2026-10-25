const std = @import("std");

pub const TokenType = enum {
    // Keywords
    kw_select,
    kw_from,
    kw_where,
    kw_insert,
    kw_into,
    kw_values,
    kw_update,
    kw_set,
    kw_delete,
    kw_create,
    kw_table,
    kw_drop,
    kw_join,
    kw_left,
    kw_inner,
    kw_on,
    kw_and,
    kw_or,
    kw_not,
    kw_order,
    kw_by,
    kw_as,
    kw_asc,
    kw_desc,
    kw_limit,
    kw_primary,
    kw_key,
    kw_null,
    kw_true,
    kw_false,
    kw_show,
    kw_tables,
    kw_describe,
    kw_peers,
    kw_sync,
    kw_status,
    kw_help,
    kw_exit,
    kw_quit,
    kw_alter,
    kw_modify,
    kw_add,
    kw_column,
    kw_rename,
    kw_to,
    kw_autoincrement,

    // Data types
    kw_int,
    kw_integer,
    kw_real,
    kw_float,
    kw_text,
    kw_bool,
    kw_boolean,
    kw_timestamp,
    kw_blob,

    // Literals & Identifiers
    identifier,
    string_literal,
    number_literal,

    // Symbols
    lparen,
    rparen,
    comma,
    semicolon,
    asterisk,
    dot,
    equal,
    not_equal,
    less,
    less_equal,
    greater,
    greater_equal,

    eof,
    invalid,
};

pub const Token = struct {
    token_type: TokenType,
    lexeme: []const u8,
    position: usize,
};

pub const Lexer = struct {
    source: []const u8,
    cursor: usize = 0,

    pub fn init(source: []const u8) Lexer {
        return Lexer{ .source = source, .cursor = 0 };
    }

    fn peek(self: *Lexer) ?u8 {
        if (self.cursor >= self.source.len) return null;
        return self.source[self.cursor];
    }

    fn advance(self: *Lexer) ?u8 {
        if (self.cursor >= self.source.len) return null;
        const c = self.source[self.cursor];
        self.cursor += 1;
        return c;
    }

    fn skipWhitespace(self: *Lexer) void {
        while (self.cursor < self.source.len) {
            const c = self.source[self.cursor];
            if (std.ascii.isWhitespace(c)) {
                self.cursor += 1;
            } else if (c == '-' and self.cursor + 1 < self.source.len and self.source[self.cursor + 1] == '-') {
                // Line comment
                while (self.cursor < self.source.len and self.source[self.cursor] != '\n') {
                    self.cursor += 1;
                }
            } else {
                break;
            }
        }
    }

    pub fn nextToken(self: *Lexer) Token {
        self.skipWhitespace();

        if (self.cursor >= self.source.len) {
            return Token{ .token_type = .eof, .lexeme = "", .position = self.cursor };
        }

        const start = self.cursor;
        const c = self.advance().?;

        switch (c) {
            '(' => return Token{ .token_type = .lparen, .lexeme = self.source[start..self.cursor], .position = start },
            ')' => return Token{ .token_type = .rparen, .lexeme = self.source[start..self.cursor], .position = start },
            ',' => return Token{ .token_type = .comma, .lexeme = self.source[start..self.cursor], .position = start },
            ';' => return Token{ .token_type = .semicolon, .lexeme = self.source[start..self.cursor], .position = start },
            '*' => return Token{ .token_type = .asterisk, .lexeme = self.source[start..self.cursor], .position = start },
            '.' => return Token{ .token_type = .dot, .lexeme = self.source[start..self.cursor], .position = start },
            '=' => return Token{ .token_type = .equal, .lexeme = self.source[start..self.cursor], .position = start },
            '!' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return Token{ .token_type = .not_equal, .lexeme = self.source[start..self.cursor], .position = start };
                }
                return Token{ .token_type = .invalid, .lexeme = self.source[start..self.cursor], .position = start };
            },
            '<' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return Token{ .token_type = .less_equal, .lexeme = self.source[start..self.cursor], .position = start };
                } else if (self.peek() == '>') {
                    _ = self.advance();
                    return Token{ .token_type = .not_equal, .lexeme = self.source[start..self.cursor], .position = start };
                }
                return Token{ .token_type = .less, .lexeme = self.source[start..self.cursor], .position = start };
            },
            '>' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return Token{ .token_type = .greater_equal, .lexeme = self.source[start..self.cursor], .position = start };
                }
                return Token{ .token_type = .greater, .lexeme = self.source[start..self.cursor], .position = start };
            },
            '\'', '"' => {
                const quote = c;
                while (self.cursor < self.source.len and self.source[self.cursor] != quote) {
                    self.cursor += 1;
                }
                if (self.cursor < self.source.len) {
                    self.cursor += 1; // consume closing quote
                    return Token{
                        .token_type = .string_literal,
                        .lexeme = self.source[start + 1 .. self.cursor - 1],
                        .position = start,
                    };
                }
                return Token{ .token_type = .invalid, .lexeme = self.source[start..self.cursor], .position = start };
            },
            else => {
                if (std.ascii.isAlphabetic(c) or c == '_') {
                    while (self.cursor < self.source.len) {
                        const next_c = self.source[self.cursor];
                        if (std.ascii.isAlphanumeric(next_c) or next_c == '_') {
                            self.cursor += 1;
                        } else {
                            break;
                        }
                    }
                    const lexeme = self.source[start..self.cursor];
                    const ttype = matchKeywordOrIdentifier(lexeme);
                    return Token{ .token_type = ttype, .lexeme = lexeme, .position = start };
                } else if (std.ascii.isDigit(c) or (c == '-' and self.peek() != null and std.ascii.isDigit(self.peek().?))) {
                    while (self.cursor < self.source.len and (std.ascii.isDigit(self.source[self.cursor]) or self.source[self.cursor] == '.')) {
                        self.cursor += 1;
                    }
                    return Token{
                        .token_type = .number_literal,
                        .lexeme = self.source[start..self.cursor],
                        .position = start,
                    };
                }
                return Token{ .token_type = .invalid, .lexeme = self.source[start..self.cursor], .position = start };
            },
        }
    }

    fn matchKeywordOrIdentifier(lexeme: []const u8) TokenType {
        if (std.ascii.eqlIgnoreCase(lexeme, "SELECT")) return .kw_select;
        if (std.ascii.eqlIgnoreCase(lexeme, "FROM")) return .kw_from;
        if (std.ascii.eqlIgnoreCase(lexeme, "WHERE")) return .kw_where;
        if (std.ascii.eqlIgnoreCase(lexeme, "INSERT")) return .kw_insert;
        if (std.ascii.eqlIgnoreCase(lexeme, "INTO")) return .kw_into;
        if (std.ascii.eqlIgnoreCase(lexeme, "VALUES")) return .kw_values;
        if (std.ascii.eqlIgnoreCase(lexeme, "UPDATE")) return .kw_update;
        if (std.ascii.eqlIgnoreCase(lexeme, "SET")) return .kw_set;
        if (std.ascii.eqlIgnoreCase(lexeme, "DELETE")) return .kw_delete;
        if (std.ascii.eqlIgnoreCase(lexeme, "CREATE")) return .kw_create;
        if (std.ascii.eqlIgnoreCase(lexeme, "TABLE")) return .kw_table;
        if (std.ascii.eqlIgnoreCase(lexeme, "DROP")) return .kw_drop;
        if (std.ascii.eqlIgnoreCase(lexeme, "JOIN")) return .kw_join;
        if (std.ascii.eqlIgnoreCase(lexeme, "LEFT")) return .kw_left;
        if (std.ascii.eqlIgnoreCase(lexeme, "INNER")) return .kw_inner;
        if (std.ascii.eqlIgnoreCase(lexeme, "ON")) return .kw_on;
        if (std.ascii.eqlIgnoreCase(lexeme, "AND")) return .kw_and;
        if (std.ascii.eqlIgnoreCase(lexeme, "OR")) return .kw_or;
        if (std.ascii.eqlIgnoreCase(lexeme, "NOT")) return .kw_not;
        if (std.ascii.eqlIgnoreCase(lexeme, "ORDER")) return .kw_order;
        if (std.ascii.eqlIgnoreCase(lexeme, "BY")) return .kw_by;
        if (std.ascii.eqlIgnoreCase(lexeme, "AS")) return .kw_as;
        if (std.ascii.eqlIgnoreCase(lexeme, "ASC")) return .kw_asc;
        if (std.ascii.eqlIgnoreCase(lexeme, "DESC")) return .kw_desc;
        if (std.ascii.eqlIgnoreCase(lexeme, "LIMIT")) return .kw_limit;
        if (std.ascii.eqlIgnoreCase(lexeme, "PRIMARY")) return .kw_primary;
        if (std.ascii.eqlIgnoreCase(lexeme, "KEY")) return .kw_key;
        if (std.ascii.eqlIgnoreCase(lexeme, "NULL")) return .kw_null;
        if (std.ascii.eqlIgnoreCase(lexeme, "TRUE")) return .kw_true;
        if (std.ascii.eqlIgnoreCase(lexeme, "FALSE")) return .kw_false;
        if (std.ascii.eqlIgnoreCase(lexeme, "SHOW")) return .kw_show;
        if (std.ascii.eqlIgnoreCase(lexeme, "TABLES")) return .kw_tables;
        if (std.ascii.eqlIgnoreCase(lexeme, "DESCRIBE")) return .kw_describe;
        if (std.ascii.eqlIgnoreCase(lexeme, "PEERS")) return .kw_peers;
        if (std.ascii.eqlIgnoreCase(lexeme, "SYNC")) return .kw_sync;
        if (std.ascii.eqlIgnoreCase(lexeme, "STATUS")) return .kw_status;
        if (std.ascii.eqlIgnoreCase(lexeme, "HELP")) return .kw_help;
        if (std.ascii.eqlIgnoreCase(lexeme, "EXIT")) return .kw_exit;
        if (std.ascii.eqlIgnoreCase(lexeme, "QUIT")) return .kw_quit;
        if (std.ascii.eqlIgnoreCase(lexeme, "ALTER")) return .kw_alter;
        if (std.ascii.eqlIgnoreCase(lexeme, "MODIFY")) return .kw_modify;
        if (std.ascii.eqlIgnoreCase(lexeme, "ADD")) return .kw_add;
        if (std.ascii.eqlIgnoreCase(lexeme, "COLUMN")) return .kw_column;
        if (std.ascii.eqlIgnoreCase(lexeme, "RENAME")) return .kw_rename;
        if (std.ascii.eqlIgnoreCase(lexeme, "TO")) return .kw_to;
        if (std.ascii.eqlIgnoreCase(lexeme, "AUTO_INCREMENT") or std.ascii.eqlIgnoreCase(lexeme, "AUTOINCREMENT")) return .kw_autoincrement;

        if (std.ascii.eqlIgnoreCase(lexeme, "INT")) return .kw_int;
        if (std.ascii.eqlIgnoreCase(lexeme, "INTEGER")) return .kw_integer;
        if (std.ascii.eqlIgnoreCase(lexeme, "REAL")) return .kw_real;
        if (std.ascii.eqlIgnoreCase(lexeme, "FLOAT")) return .kw_float;
        if (std.ascii.eqlIgnoreCase(lexeme, "TEXT")) return .kw_text;
        if (std.ascii.eqlIgnoreCase(lexeme, "BOOL")) return .kw_bool;
        if (std.ascii.eqlIgnoreCase(lexeme, "BOOLEAN")) return .kw_boolean;
        if (std.ascii.eqlIgnoreCase(lexeme, "TIMESTAMP")) return .kw_timestamp;
        if (std.ascii.eqlIgnoreCase(lexeme, "BLOB")) return .kw_blob;

        return .identifier;
    }
};
