const std = @import("std");
const types = @import("../types.zig");
const Value = types.Value;
const Row = types.Row;
const Schema = types.Schema;
const Column = types.Column;
const Engine = @import("engine.zig").Engine;

pub const DEFAULT_EMBEDDING_DIM: usize = 384;

// ============================================================================
// Vector Math and Distance Metrics
// ============================================================================

/// Calculates cosine similarity between two float vectors.
/// Result is in range [-1.0, 1.0]. Returns 0.0 if vectors have different dimensions or zero norm.
pub fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len or a.len == 0) return 0.0;

    var dot: f64 = 0.0;
    var norm_a_sq: f64 = 0.0;
    var norm_b_sq: f64 = 0.0;

    for (a, 0..) |val_a, i| {
        const val_b = b[i];
        const va: f64 = @floatCast(val_a);
        const vb: f64 = @floatCast(val_b);
        dot += va * vb;
        norm_a_sq += va * va;
        norm_b_sq += vb * vb;
    }

    if (norm_a_sq <= 1e-12 or norm_b_sq <= 1e-12) return 0.0;
    const denom = @sqrt(norm_a_sq) * @sqrt(norm_b_sq);
    const sim: f32 = @floatCast(dot / denom);
    return std.math.clamp(sim, -1.0, 1.0);
}

/// Calculates Euclidean (L2) distance between two float vectors.
pub fn l2Distance(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len or a.len == 0) return std.math.inf(f32);

    var sum_sq: f64 = 0.0;
    for (a, 0..) |val_a, i| {
        const diff: f64 = @as(f64, @floatCast(val_a)) - @as(f64, @floatCast(b[i]));
        sum_sq += diff * diff;
    }
    return @floatCast(@sqrt(sum_sq));
}

/// Calculates Dot Product (Inner Product) between two float vectors.
pub fn dotProduct(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len or a.len == 0) return 0.0;

    var dot: f64 = 0.0;
    for (a, 0..) |val_a, i| {
        dot += @as(f64, @floatCast(val_a)) * @as(f64, @floatCast(b[i]));
    }
    return @floatCast(dot);
}

/// Calculates Manhattan (L1) distance between two float vectors.
pub fn manhattanDistance(a: []const f32, b: []const f32) f32 {
    if (a.len != b.len or a.len == 0) return std.math.inf(f32);

    var sum: f64 = 0.0;
    for (a, 0..) |val_a, i| {
        const diff: f64 = @abs(@as(f64, @floatCast(val_a)) - @as(f64, @floatCast(b[i])));
        sum += diff;
    }
    return @floatCast(sum);
}

/// Calculates L2 Norm (Euclidean length) of a float vector.
pub fn l2Norm(vec: []const f32) f32 {
    var sum_sq: f64 = 0.0;
    for (vec) |v| {
        const fv: f64 = @floatCast(v);
        sum_sq += fv * fv;
    }
    return @floatCast(@sqrt(sum_sq));
}

/// Normalizes a float vector to unit length (L2 norm = 1.0) in place.
pub fn normalize(vec: []f32) void {
    const norm = l2Norm(vec);
    if (norm <= 1e-12) return;
    const inv = 1.0 / norm;
    for (vec) |*v| {
        v.* *= inv;
    }
}

// ============================================================================
// Vector String Parsing and Formatting
// ============================================================================

/// Parses string representation of vector (e.g. "[0.12, -0.45, 0.78]" or "0.12, -0.45, 0.78")
pub fn parseVector(allocator: std.mem.Allocator, str: []const u8) ![]f32 {
    var trimmed = std.mem.trim(u8, str, " \t\r\n");
    if (trimmed.len >= 2 and trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']') {
        trimmed = trimmed[1 .. trimmed.len - 1];
    } else if (trimmed.len >= 2 and trimmed[0] == '(' and trimmed[trimmed.len - 1] == ')') {
        trimmed = trimmed[1 .. trimmed.len - 1];
    }

    trimmed = std.mem.trim(u8, trimmed, " \t\r\n");
    if (trimmed.len == 0) {
        return try allocator.alloc(f32, 0);
    }

    var list = std.ArrayList(f32).init(allocator);
    errdefer list.deinit();

    var it = std.mem.splitAny(u8, trimmed, ", \t\n\r");
    while (it.next()) |part| {
        const item = std.mem.trim(u8, part, " \t\r\n");
        if (item.len == 0) continue;
        const val = std.fmt.parseFloat(f32, item) catch return error.InvalidVectorFloat;
        try list.append(val);
    }

    return try list.toOwnedSlice();
}

/// Formats a float vector as JSON array string "[0.1234, -0.5678, ...]"
pub fn formatVector(allocator: std.mem.Allocator, vec: []const f32, max_elements: ?usize) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    try buf.append('[');
    const limit = if (max_elements) |m| @min(vec.len, m) else vec.len;

    for (vec[0..limit], 0..) |v, i| {
        if (i > 0) try buf.appendSlice(", ");
        try buf.writer().print("{d:.4}", .{v});
    }

    if (limit < vec.len) {
        try buf.appendSlice(", ...");
    }
    try buf.append(']');

    return try buf.toOwnedSlice();
}

// ============================================================================
// Built-in Deterministic Semantic Vectorizer
// Zero external dependencies. Fast, reproducible 384-dimensional dense embeddings.
// ============================================================================

pub const SemanticEmbedder = struct {
    pub const DIM: usize = DEFAULT_EMBEDDING_DIM;

    /// Embeds arbitrary text into a normalized 384-dimensional dense semantic vector.
    pub fn embedText(allocator: std.mem.Allocator, text: []const u8) ![]f32 {
        const vec = try allocator.alloc(f32, DIM);
        @memset(vec, 0.0);

        if (text.len == 0) {
            return vec;
        }

        var word_count: f32 = 0.0;
        var cursor: usize = 0;

        while (cursor < text.len) {
            // Skip non-alphanumeric separators
            while (cursor < text.len and isSeparator(text[cursor])) : (cursor += 1) {}
            if (cursor >= text.len) break;

            const start = cursor;
            while (cursor < text.len and !isSeparator(text[cursor])) : (cursor += 1) {}
            const word = text[start..cursor];
            if (word.len == 0) continue;

            word_count += 1.0;
            // Positional damping: early terms have slightly higher salience
            const pos_weight: f32 = 1.0 / (1.0 + 0.005 * word_count);

            // Lowercase normalized word buffer (up to 64 bytes)
            var lower_buf: [64]u8 = undefined;
            const w_len = @min(word.len, lower_buf.len);
            for (word[0..w_len], 0..) |c, i| {
                lower_buf[i] = std.ascii.toLower(c);
            }
            const norm_word = lower_buf[0..w_len];

            // 1. Whole-word feature hashing across 3 seeds
            hashAndAccumulate(vec, norm_word, 0x811c9dc5, 1.8 * pos_weight);
            hashAndAccumulate(vec, norm_word, 0x9e3779b9, 1.4 * pos_weight);
            hashAndAccumulate(vec, norm_word, 0x517cc1b7, 1.2 * pos_weight);

            // 2. Character n-gram hashing (3-grams, 4-grams) for subword semantic capture
            if (norm_word.len >= 3) {
                var i: usize = 0;
                while (i + 3 <= norm_word.len) : (i += 1) {
                    const trigram = norm_word[i .. i + 3];
                    hashAndAccumulate(vec, trigram, 0x27d4eb2d, 0.7 * pos_weight);
                }
            }
            if (norm_word.len >= 4) {
                var i: usize = 0;
                while (i + 4 <= norm_word.len) : (i += 1) {
                    const fourgram = norm_word[i .. i + 4];
                    hashAndAccumulate(vec, fourgram, 0x165667b1, 0.85 * pos_weight);
                }
            }
        }

        // Apply smooth non-linear compression (soft sign: x / (1 + |x|)) to prevent outlier dominance
        for (vec) |*v| {
            const x = v.*;
            v.* = x / (1.0 + @abs(x));
        }

        // L2 unit normalization so cosine similarity is a simple dot product
        normalize(vec);
        return vec;
    }

    fn isSeparator(c: u8) bool {
        return c <= ' ' or c == ',' or c == '.' or c == ';' or c == ':' or
            c == '(' or c == ')' or c == '[' or c == ']' or c == '{' or
            c == '}' or c == '"' or c == '\'' or c == '`' or c == '/' or
            c == '\\' or c == '|' or c == '!' or c == '?' or c == '-' or
            c == '_' or c == '=' or c == '+' or c == '*' or c == '&';
    }

    fn hashAndAccumulate(vec: []f32, token: []const u8, seed: u32, weight: f32) void {
        var h: u32 = seed;
        for (token) |b| {
            h ^= b;
            h = h *% 0x01000193; // 32-bit FNV prime
        }

        const idx = h % DIM;
        // Deterministic sign (+1 or -1) from upper bits
        const sign: f32 = if ((h & 0x80000000) != 0) 1.0 else -1.0;
        vec[idx] += sign * weight;
    }
};

// ============================================================================
// Text Chunker for Document Infeed
// Hierarchically splits documents into chunks with sliding window overlap.
// ============================================================================

pub const TextChunk = struct {
    chunk_index: usize,
    content: []const u8,
    start_offset: usize,
    end_offset: usize,
};

pub const TextChunker = struct {
    pub fn chunkText(
        allocator: std.mem.Allocator,
        text: []const u8,
        target_chunk_size: usize,
        target_overlap: usize,
    ) ![]TextChunk {
        const chunk_size = @max(50, target_chunk_size);
        const overlap = @min(chunk_size / 2, target_overlap);

        const trimmed = std.mem.trim(u8, text, " \t\r\n");
        if (trimmed.len == 0) {
            return try allocator.alloc(TextChunk, 0);
        }

        if (trimmed.len <= chunk_size) {
            const chunks = try allocator.alloc(TextChunk, 1);
            chunks[0] = TextChunk{
                .chunk_index = 0,
                .content = try allocator.dupe(u8, trimmed),
                .start_offset = 0,
                .end_offset = trimmed.len,
            };
            return chunks;
        }

        var list = std.ArrayList(TextChunk).init(allocator);
        errdefer {
            for (list.items) |c| allocator.free(c.content);
            list.deinit();
        }

        var start: usize = 0;
        var chunk_idx: usize = 0;

        while (start < trimmed.len) {
            var end = @min(start + chunk_size, trimmed.len);

            // If not at the very end, try finding a natural boundary
            if (end < trimmed.len) {
                const search_window_start = if (end > start + (chunk_size / 2)) end - (chunk_size / 3) else start;
                const window = trimmed[search_window_start..end];

                // Boundary priority: 1. Paragraph (\n\n), 2. Line (\n), 3. Sentence (. ), 4. Space
                if (std.mem.lastIndexOf(u8, window, "\n\n")) |rel_p| {
                    end = search_window_start + rel_p + 2;
                } else if (std.mem.lastIndexOf(u8, window, "\n")) |rel_nl| {
                    end = search_window_start + rel_nl + 1;
                } else if (std.mem.lastIndexOf(u8, window, ". ")) |rel_dot| {
                    end = search_window_start + rel_dot + 2;
                } else if (std.mem.lastIndexOf(u8, window, "? ")) |rel_q| {
                    end = search_window_start + rel_q + 2;
                } else if (std.mem.lastIndexOf(u8, window, "! ")) |rel_ex| {
                    end = search_window_start + rel_ex + 2;
                } else if (std.mem.lastIndexOfScalar(u8, window, ' ')) |rel_sp| {
                    end = search_window_start + rel_sp + 1;
                }
            }

            const raw_chunk = trimmed[start..end];
            const clean_chunk = std.mem.trim(u8, raw_chunk, " \t\r\n");

            if (clean_chunk.len > 0) {
                try list.append(TextChunk{
                    .chunk_index = chunk_idx,
                    .content = try allocator.dupe(u8, clean_chunk),
                    .start_offset = start,
                    .end_offset = end,
                });
                chunk_idx += 1;
            }

            if (end >= trimmed.len) break;

            // Advance start position with overlap
            const step = if (end > start + overlap) end - overlap else end;
            start = @max(start + 1, step);
        }

        return try list.toOwnedSlice();
    }
};

// ============================================================================
// RAG Knowledge Base and Vector Infeed Manager
// ============================================================================

pub const InfeedResult = struct {
    collection: []const u8,
    doc_id: []const u8,
    doc_title: []const u8,
    chunks_created: usize,
    total_characters: usize,
    dimension: usize,

    pub fn deinit(self: InfeedResult, allocator: std.mem.Allocator) void {
        allocator.free(self.collection);
        allocator.free(self.doc_id);
        allocator.free(self.doc_title);
    }
};

pub const RagChunkMatch = struct {
    chunk_id: []const u8,
    doc_id: []const u8,
    doc_title: []const u8,
    chunk_index: i64,
    content: []const u8,
    score: f32,
    metadata: []const u8,

    pub fn deinit(self: RagChunkMatch, allocator: std.mem.Allocator) void {
        allocator.free(self.chunk_id);
        allocator.free(self.doc_id);
        allocator.free(self.doc_title);
        allocator.free(self.content);
        allocator.free(self.metadata);
    }
};

pub const RagQueryResult = struct {
    query: []const u8,
    collection: []const u8,
    matches: []RagChunkMatch,
    context_prompt: []const u8,

    pub fn deinit(self: RagQueryResult, allocator: std.mem.Allocator) void {
        allocator.free(self.query);
        allocator.free(self.collection);
        for (self.matches) |m| m.deinit(allocator);
        allocator.free(self.matches);
        allocator.free(self.context_prompt);
    }
};

pub const CollectionInfo = struct {
    name: []const u8,
    table_name: []const u8,
    chunk_count: usize,
    doc_count: usize,
    dimension: usize,
    created_at: i64,

    pub fn deinit(self: CollectionInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.table_name);
    }
};

pub const RagManager = struct {
    allocator: std.mem.Allocator,
    engine: *Engine,

    pub fn init(allocator: std.mem.Allocator, engine: *Engine) RagManager {
        return RagManager{
            .allocator = allocator,
            .engine = engine,
        };
    }

    /// Converts collection name into internal table name (e.g. "manuals" -> "_rag_manuals")
    pub fn getCollectionTableName(self: *RagManager, collection: []const u8) ![]u8 {
        const clean = std.mem.trim(u8, collection, " \t\r\n_");
        const safe_name = if (clean.len == 0) "default" else clean;
        return try std.fmt.allocPrint(self.allocator, "_rag_{s}", .{safe_name});
    }

    /// Ensures the RAG collection table exists with the standard vector schema.
    pub fn ensureCollectionTable(self: *RagManager, collection: []const u8) ![]u8 {
        const table_name = try self.getCollectionTableName(collection);
        errdefer self.allocator.free(table_name);

        if (self.engine.getTable(table_name)) |_| {
            return table_name;
        }

        // Schema definition for RAG collections:
        // chunk_id TEXT PRIMARY KEY, doc_id TEXT, doc_title TEXT, chunk_index INT,
        // content TEXT, embedding VECTOR, metadata TEXT, created_at TIMESTAMP
        const cols = try self.allocator.alloc(Column, 8);
        defer self.allocator.free(cols);

        cols[0] = Column{ .name = "chunk_id", .data_type = .text, .primary_key = true, .nullable = false };
        cols[1] = Column{ .name = "doc_id", .data_type = .text, .nullable = false };
        cols[2] = Column{ .name = "doc_title", .data_type = .text, .nullable = false };
        cols[3] = Column{ .name = "chunk_index", .data_type = .integer, .nullable = false };
        cols[4] = Column{ .name = "content", .data_type = .text, .nullable = false };
        cols[5] = Column{ .name = "embedding", .data_type = .vector, .nullable = false };
        cols[6] = Column{ .name = "metadata", .data_type = .text, .nullable = true };
        cols[7] = Column{ .name = "created_at", .data_type = .timestamp, .nullable = false };

        const schema = try Schema.init(self.allocator, cols);
        defer schema.deinit(self.allocator);

        _ = try self.engine.createTable(table_name, schema);
        return table_name;
    }

    /// Infeeds raw document text into a RAG collection with automatic chunking and embedding.
    pub fn infeedDocument(
        self: *RagManager,
        collection: []const u8,
        title: []const u8,
        source: []const u8,
        text: []const u8,
        chunk_size: usize,
        chunk_overlap: usize,
        custom_embeddings: ?[][]const f32,
    ) !InfeedResult {
        const table_name = try self.ensureCollectionTable(collection);
        defer self.allocator.free(table_name);

        const chunks = try TextChunker.chunkText(self.allocator, text, chunk_size, chunk_overlap);
        defer {
            for (chunks) |c| self.allocator.free(c.content);
            self.allocator.free(chunks);
        }

        const now = std.time.timestamp();
        // Generate doc_id
        var rand_bytes: [8]u8 = undefined;
        std.crypto.random.bytes(&rand_bytes);
        const doc_id = try std.fmt.allocPrint(self.allocator, "doc_{s}_{x}", .{
            std.mem.trim(u8, title, " \t\r\n")[0..@min(12, title.len)],
            std.fmt.fmtSliceHexLower(&rand_bytes),
        });
        errdefer self.allocator.free(doc_id);

        for (chunks, 0..) |chunk, i| {
            const chunk_id = try std.fmt.allocPrint(self.allocator, "{s}_c{d}", .{ doc_id, chunk.chunk_index });
            defer self.allocator.free(chunk_id);

            // Compute or take embedding
            const embedding_slice = if (custom_embeddings) |ce|
                if (i < ce.len) try self.allocator.dupe(f32, ce[i]) else try SemanticEmbedder.embedText(self.allocator, chunk.content)
            else
                try SemanticEmbedder.embedText(self.allocator, chunk.content);
            errdefer self.allocator.free(embedding_slice);

            const meta_json = try std.fmt.allocPrint(
                self.allocator,
                "{{\"source\":\"{s}\",\"start\":{d},\"end\":{d},\"chars\":{d}}}",
                .{ source, chunk.start_offset, chunk.end_offset, chunk.content.len },
            );
            defer self.allocator.free(meta_json);

            var row = try Row.init(self.allocator, 8);
            row.values[0] = Value{ .text = try self.allocator.dupe(u8, chunk_id) };
            row.values[1] = Value{ .text = try self.allocator.dupe(u8, doc_id) };
            row.values[2] = Value{ .text = try self.allocator.dupe(u8, title) };
            row.values[3] = Value{ .integer = @intCast(chunk.chunk_index) };
            row.values[4] = Value{ .text = try self.allocator.dupe(u8, chunk.content) };
            row.values[5] = Value{ .vector = embedding_slice };
            row.values[6] = Value{ .text = try self.allocator.dupe(u8, meta_json) };
            row.values[7] = Value{ .timestamp = now };

            try self.engine.insertRow(table_name, row);
            row.deinit(self.allocator);
        }

        return InfeedResult{
            .collection = try self.allocator.dupe(u8, collection),
            .doc_id = doc_id,
            .doc_title = try self.allocator.dupe(u8, title),
            .chunks_created = chunks.len,
            .total_characters = text.len,
            .dimension = DEFAULT_EMBEDDING_DIM,
        };
    }

    /// Infeeds and vectorizes an existing relational table into a RAG collection.
    pub fn infeedTable(
        self: *RagManager,
        source_table_name: []const u8,
        text_columns: []const []const u8,
        id_column: ?[]const u8,
        collection: []const u8,
        max_rows: ?usize,
    ) !InfeedResult {
        const src_table = self.engine.getTable(source_table_name) orelse return error.TableNotFound;

        src_table.mutex.lock();
        defer src_table.mutex.unlock();

        const table_name = try self.ensureCollectionTable(collection);
        defer self.allocator.free(table_name);

        const limit = if (max_rows) |m| @min(src_table.rows.items.len, m) else src_table.rows.items.len;
        const now = std.time.timestamp();

        var text_buf = std.ArrayList(u8).init(self.allocator);
        defer text_buf.deinit();

        var inserted_count: usize = 0;
        var total_chars: usize = 0;

        for (src_table.rows.items[0..limit], 0..) |row, r_idx| {
            text_buf.clearRetainingCapacity();

            // Build human-readable descriptive text from selected columns
            for (text_columns, 0..) |cname, c_i| {
                if (src_table.schema.getColumnIndex(cname)) |col_idx| {
                    if (c_i > 0) try text_buf.appendSlice(" | ");
                    try text_buf.writer().print("{s}: ", .{cname});
                    const val_str = try row.values[col_idx].toStringAlloc(self.allocator);
                    defer self.allocator.free(val_str);
                    try text_buf.appendSlice(val_str);
                }
            }

            if (text_buf.items.len == 0) continue;
            total_chars += text_buf.items.len;

            // Determine item ID
            var row_id_str: []const u8 = undefined;
            if (id_column) |id_cname| {
                if (src_table.schema.getColumnIndex(id_cname)) |pk_idx| {
                    row_id_str = try row.values[pk_idx].toStringAlloc(self.allocator);
                } else {
                    row_id_str = try std.fmt.allocPrint(self.allocator, "row_{d}", .{r_idx + 1});
                }
            } else {
                row_id_str = try std.fmt.allocPrint(self.allocator, "row_{d}", .{r_idx + 1});
            }
            defer self.allocator.free(row_id_str);

            const chunk_id = try std.fmt.allocPrint(self.allocator, "tbl_{s}_{s}", .{ source_table_name, row_id_str });
            defer self.allocator.free(chunk_id);

            const doc_title = try std.fmt.allocPrint(self.allocator, "{s} #{s}", .{ source_table_name, row_id_str });
            defer self.allocator.free(doc_title);

            const embedding = try SemanticEmbedder.embedText(self.allocator, text_buf.items);
            errdefer self.allocator.free(embedding);

            const meta_json = try std.fmt.allocPrint(
                self.allocator,
                "{{\"source\":\"table:{s}\",\"row_id\":\"{s}\",\"chars\":{d}}}",
                .{ source_table_name, row_id_str, text_buf.items.len },
            );
            defer self.allocator.free(meta_json);

            var r = try Row.init(self.allocator, 8);
            r.values[0] = Value{ .text = try self.allocator.dupe(u8, chunk_id) };
            r.values[1] = Value{ .text = try self.allocator.dupe(u8, row_id_str) };
            r.values[2] = Value{ .text = try self.allocator.dupe(u8, doc_title) };
            r.values[3] = Value{ .integer = @intCast(r_idx) };
            r.values[4] = Value{ .text = try self.allocator.dupe(u8, text_buf.items) };
            r.values[5] = Value{ .vector = embedding };
            r.values[6] = Value{ .text = try self.allocator.dupe(u8, meta_json) };
            r.values[7] = Value{ .timestamp = now };

            try self.engine.insertRow(table_name, r);
            r.deinit(self.allocator);
            inserted_count += 1;
        }

        return InfeedResult{
            .collection = try self.allocator.dupe(u8, collection),
            .doc_id = try std.fmt.allocPrint(self.allocator, "table_{s}", .{source_table_name}),
            .doc_title = try std.fmt.allocPrint(self.allocator, "Table {s}", .{source_table_name}),
            .chunks_created = inserted_count,
            .total_characters = total_chars,
            .dimension = DEFAULT_EMBEDDING_DIM,
        };
    }

    /// Performs semantic vector search on a RAG collection and synthesizes the LLM context prompt.
    pub fn queryRag(
        self: *RagManager,
        collection: []const u8,
        query_text: []const u8,
        query_vec: ?[]const f32,
        top_k: usize,
        min_score: f32,
    ) !RagQueryResult {
        const table_name = try self.getCollectionTableName(collection);
        defer self.allocator.free(table_name);

        const table = self.engine.getTable(table_name) orelse return error.CollectionNotFound;

        table.mutex.lock();
        defer table.mutex.unlock();

        // Obtain query vector
        var own_qvec: ?[]f32 = null;
        const qvec: []const f32 = if (query_vec) |qv| qv else blk: {
            own_qvec = try SemanticEmbedder.embedText(self.allocator, query_text);
            break :blk own_qvec.?;
        };
        defer if (own_qvec) |ov| self.allocator.free(ov);

        const chunk_id_idx = table.schema.getColumnIndex("chunk_id") orelse return error.InvalidRagSchema;
        const doc_id_idx = table.schema.getColumnIndex("doc_id") orelse return error.InvalidRagSchema;
        const doc_title_idx = table.schema.getColumnIndex("doc_title") orelse return error.InvalidRagSchema;
        const chunk_idx_idx = table.schema.getColumnIndex("chunk_index") orelse return error.InvalidRagSchema;
        const content_idx = table.schema.getColumnIndex("content") orelse return error.InvalidRagSchema;
        const embedding_idx = table.schema.getColumnIndex("embedding") orelse return error.InvalidRagSchema;
        const metadata_idx = table.schema.getColumnIndex("metadata") orelse return error.InvalidRagSchema;

        // Temporary scored match struct for sorting
        const Scored = struct {
            row_idx: usize,
            score: f32,
        };

        var candidates = std.ArrayList(Scored).init(self.allocator);
        defer candidates.deinit();

        for (table.rows.items, 0..) |row, r_i| {
            const emb_val = row.values[embedding_idx];
            if (emb_val != .vector) continue;
            const sim = cosineSimilarity(qvec, emb_val.vector);
            if (sim >= min_score) {
                try candidates.append(.{ .row_idx = r_i, .score = sim });
            }
        }

        // Sort descending by score
        std.mem.sort(Scored, candidates.items, {}, struct {
            fn order(_: void, a: Scored, b: Scored) bool {
                return a.score > b.score;
            }
        }.order);

        const k = @min(candidates.items.len, @max(1, top_k));
        var matches = try self.allocator.alloc(RagChunkMatch, k);
        errdefer self.allocator.free(matches);

        var match_idx: usize = 0;
        errdefer {
            for (matches[0..match_idx]) |m| m.deinit(self.allocator);
        }

        while (match_idx < k) : (match_idx += 1) {
            const item = candidates.items[match_idx];
            const row = table.rows.items[item.row_idx];

            const cid_str = try row.values[chunk_id_idx].toStringAlloc(self.allocator);
            errdefer self.allocator.free(cid_str);
            const did_str = try row.values[doc_id_idx].toStringAlloc(self.allocator);
            errdefer self.allocator.free(did_str);
            const title_str = try row.values[doc_title_idx].toStringAlloc(self.allocator);
            errdefer self.allocator.free(title_str);
            const cindex = if (row.values[chunk_idx_idx] == .integer) row.values[chunk_idx_idx].integer else 0;
            const cnt_str = try row.values[content_idx].toStringAlloc(self.allocator);
            errdefer self.allocator.free(cnt_str);
            const meta_str = try row.values[metadata_idx].toStringAlloc(self.allocator);
            errdefer self.allocator.free(meta_str);

            matches[match_idx] = RagChunkMatch{
                .chunk_id = cid_str,
                .doc_id = did_str,
                .doc_title = title_str,
                .chunk_index = cindex,
                .content = cnt_str,
                .score = item.score,
                .metadata = meta_str,
            };
        }

        // Build formatted RAG Context Prompt for LLMs
        var prompt_buf = std.ArrayList(u8).init(self.allocator);
        errdefer prompt_buf.deinit();

        try prompt_buf.appendSlice(
            \\=== RETRIEVAL-AUGMENTED GENERATION (RAG) CONTEXT ===
            \\You are provided the following verified excerpts retrieved from the StrataDB vector knowledge base.
            \\Use these excerpts to answer the user query accurately. Cite sources using [1], [2], etc.
            \\
            \\
        );

        for (matches, 0..) |m, i| {
            try prompt_buf.writer().print(
                \\[{d}] Source: "{s}" (Chunk #{d}, Similarity: {d:.1}%):
                \\{s}
                \\
                \\
            ,
                .{ i + 1, m.doc_title, m.chunk_index, m.score * 100.0, m.content },
            );
        }

        try prompt_buf.writer().print(
            \\=== USER QUERY ===
            \\{s}
            \\==================================================
        , .{query_text});

        return RagQueryResult{
            .query = try self.allocator.dupe(u8, query_text),
            .collection = try self.allocator.dupe(u8, collection),
            .matches = matches,
            .context_prompt = try prompt_buf.toOwnedSlice(),
        };
    }

    /// Lists all active RAG collections and their statistics.
    pub fn listCollections(self: *RagManager) ![]CollectionInfo {
        self.engine.mutex.lock();
        defer self.engine.mutex.unlock();

        var list = std.ArrayList(CollectionInfo).init(self.allocator);
        errdefer {
            for (list.items) |c| c.deinit(self.allocator);
            list.deinit();
        }

        var it = self.engine.tables.iterator();
        while (it.next()) |entry| {
            const tname = entry.key_ptr.*;
            if (std.mem.startsWith(u8, tname, "_rag_")) {
                const coll_name = tname[5..];
                const tbl = entry.value_ptr.*;

                tbl.mutex.lock();
                defer tbl.mutex.unlock();

                // Unique documents count
                var doc_set = std.StringHashMap(void).init(self.allocator);
                defer doc_set.deinit();

                if (tbl.schema.getColumnIndex("doc_id")) |did_idx| {
                    for (tbl.rows.items) |r| {
                        if (r.values[did_idx] == .text) {
                            doc_set.put(r.values[did_idx].text, {}) catch {};
                        }
                    }
                }

                try list.append(CollectionInfo{
                    .name = try self.allocator.dupe(u8, coll_name),
                    .table_name = try self.allocator.dupe(u8, tname),
                    .chunk_count = tbl.rows.items.len,
                    .doc_count = doc_set.count(),
                    .dimension = DEFAULT_EMBEDDING_DIM,
                    .created_at = std.time.timestamp(),
                });
            }
        }

        return try list.toOwnedSlice();
    }

    /// Deletes a RAG collection table.
    pub fn deleteCollection(self: *RagManager, collection: []const u8) !bool {
        const table_name = try self.getCollectionTableName(collection);
        defer self.allocator.free(table_name);
        return try self.engine.dropTable(table_name);
    }
};

// ============================================================================
// Unit Tests for Vector Math and Embeddings
// ============================================================================

test "vector: cosine similarity and distances" {
    const a = [_]f32{ 1.0, 0.0, 0.0 };
    const b = [_]f32{ 1.0, 0.0, 0.0 };
    const c = [_]f32{ 0.0, 1.0, 0.0 };

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), cosineSimilarity(&a, &b), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), cosineSimilarity(&a, &c), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), l2Distance(&a, &b), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.41421), l2Distance(&a, &c), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), dotProduct(&a, &b), 0.0001);
}

test "vector: parse and format vector string" {
    const allocator = std.testing.allocator;
    const str = "[0.25, -0.75, 1.5]";
    const vec = try parseVector(allocator, str);
    defer allocator.free(vec);

    try std.testing.expectEqual(@as(usize, 3), vec.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), vec[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, -0.75), vec[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), vec[2], 0.0001);

    const formatted = try formatVector(allocator, vec, null);
    defer allocator.free(formatted);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "0.25") != null);
}

test "vector: semantic embedder consistency and semantic similarity" {
    const allocator = std.testing.allocator;

    const t1 = "Rotor shaft bearing clearance and maintenance";
    const t2 = "Maintenance procedure for rotor shaft bearing clearance";
    const t3 = "Chocolate chip cupcake baking recipe with vanilla cream";

    const v1 = try SemanticEmbedder.embedText(allocator, t1);
    defer allocator.free(v1);
    const v2 = try SemanticEmbedder.embedText(allocator, t2);
    defer allocator.free(v2);
    const v3 = try SemanticEmbedder.embedText(allocator, t3);
    defer allocator.free(v3);

    try std.testing.expectEqual(SemanticEmbedder.DIM, v1.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), l2Norm(v1), 0.01);

    const sim_related = cosineSimilarity(v1, v2);
    const sim_unrelated = cosineSimilarity(v1, v3);

    // Semantically and lexically related sentences must have noticeably higher similarity
    try std.testing.expect(sim_related > 0.75);
    try std.testing.expect(sim_unrelated < 0.35);
    try std.testing.expect(sim_related > sim_unrelated + 0.40);
}

test "vector: text chunker with overlap" {
    const allocator = std.testing.allocator;
    const text =
        \\Paragraph 1: Centrifugal pumps require periodic inspection of bearings and mechanical seals.
        \\
        \\Paragraph 2: When disassembling the casing, ensure all pressure has been safely relieved.
        \\
        \\Paragraph 3: Inspect the impeller for cavitation pitting, erosion, or foreign object damage.
    ;

    const chunks = try TextChunker.chunkText(allocator, text, 120, 20);
    defer {
        for (chunks) |c| allocator.free(c.content);
        allocator.free(chunks);
    }

    try std.testing.expect(chunks.len >= 2);
    for (chunks, 0..) |c, i| {
        try std.testing.expectEqual(i, c.chunk_index);
        try std.testing.expect(c.content.len > 0);
    }
}
