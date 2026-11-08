# StrataDB Model Context Protocol (MCP) Server

This directory provides the official Model Context Protocol (MCP) server for **StrataDB**. It connects AI models, agentic IDEs, and assistants (Google Antigravity, Claude Desktop, Cursor, and custom LLMs) to StrataDB's native 384-dimensional dense vector engine, RAG knowledge base, and relational database.

---

## Available MCP Tools

### 1. `search_knowledge_base` (Primary RAG Tool)
Performs semantic vector search across StrataDB collections using 384-D dense embeddings and cosine similarity. Returns top-ranked matching excerpts and an auto-assembled markdown context prompt complete with numeric source citations (`[1]`, `[2]`, etc.) ready for LLM synthesis.

**Parameters:**
- `query` (string, required): Natural language search query, question, symptom, or topic.
- `collection` (string, optional): Specific collection name. If omitted, all active collections in StrataDB are automatically queried and top results are merged.
- `top_k` (integer, optional, default: 3): Maximum number of top excerpts to retrieve.
- `min_score` (number, optional, default: 0.2): Minimum cosine similarity score threshold (0.0 to 1.0).
- `endpoint_url` (string, optional): Base URL of StrataDB (defaults to `STRATADB_URL` env or `http://127.0.0.1:8080`).

### 2. `list_knowledge_collections`
Lists all active RAG vector collections in StrataDB with chunk counts, document counts, vector dimensions (384-D), and creation timestamps.

### 3. `infeed_document`
Infeeds an unstructured text document into a vector collection. Automatically applies sliding-window chunking, computes dense embeddings, and stores chunks with metadata.

### 4. `infeed_table`
Extracts rows from any relational table or view, formats selected columns into structured knowledge documents, generates dense embeddings, and indexes them into a target collection.

### 5. `execute_sql`
Executes SQL queries directly against StrataDB (e.g. `SELECT`, `INSERT`, `UPDATE`, `CREATE TABLE`, `SHOW TABLES`).

---

## Configuration

### Antigravity / Claude Desktop / Cursor (`mcp_config.json`)

Add the following to your `mcp_config.json` (e.g. `~/.gemini/config/mcp_config.json` or `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "stratadb": {
      "command": "node",
      "args": [
        "/Users/martinaldrich/Documents/dtapp/mcp/server.js"
      ],
      "env": {
        "STRATADB_URL": "http://127.0.0.1:8080"
      }
    }
  }
}
```

---

## Architecture & Zero Dependencies

- **Pure Stdio Transport**: Communicates using JSON-RPC 2.0 over standard input/output.
- **Zero External npm Dependencies**: Built with native Node.js runtime primitives (`fetch`, `readline`).
- **Resilient**: Gracefully reports friendly error messages if StrataDB is temporarily offline or when collections are empty.
