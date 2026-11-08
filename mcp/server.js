#!/usr/bin/env node
/**
 * StrataDB Model Context Protocol (MCP) Server
 *
 * Exposes StrataDB's native vector database, RAG knowledge base, and relational
 * SQL engine to AI models and agentic assistants over standard JSON-RPC 2.0 Stdio transport.
 *
 * Zero external npm dependencies (uses native Node.js fetch and readline).
 */

// Route all diagnostic logging to stderr so stdout is strictly dedicated to JSON-RPC messages
console.log = (...args) => console.error(...args);
console.info = (...args) => console.error(...args);

const readline = require('readline');

const STRATADB_URL = (process.env.STRATADB_URL || 'http://127.0.0.1:8080').replace(/\/+$/, '');

const TOOLS = [
    {
        name: 'search_knowledge_base',
        description: 'Performs semantic vector search across the StrataDB RAG knowledge base. Uses 384-dimensional dense semantic embeddings and cosine similarity to retrieve relevant document chunks, technical manuals, or relational table records. Returns ranked excerpts with similarity scores and an auto-assembled context prompt with source citations for LLM synthesis.',
        inputSchema: {
            type: 'object',
            properties: {
                query: {
                    type: 'string',
                    description: 'Natural language search query, question, symptom, or topic to search for in the knowledge base (e.g. "centrifugal pump bearing overheating troubleshooting", "ISO 281 bearing standards").'
                },
                collection: {
                    type: 'string',
                    description: 'Optional name of a specific vector collection to search (e.g. "plant_manuals", "equipment_docs"). If omitted, all active vector collections in StrataDB are automatically queried and top results are merged.'
                },
                top_k: {
                    type: 'integer',
                    description: 'Maximum number of top matching excerpts to return (default: 3, range: 1-20).'
                },
                min_score: {
                    type: 'number',
                    description: 'Minimum cosine similarity score threshold (0.0 to 1.0, default: 0.2). Chunks with lower similarity are filtered out.'
                },
                endpoint_url: {
                    type: 'string',
                    description: 'Optional StrataDB server base URL (default: http://127.0.0.1:8080 or STRATADB_URL env var).'
                }
            },
            required: ['query']
        }
    },
    {
        name: 'list_knowledge_collections',
        description: 'Lists all active RAG vector collections in StrataDB, including chunk counts, document counts, vector dimensions (384-D), and creation timestamps.',
        inputSchema: {
            type: 'object',
            properties: {
                endpoint_url: {
                    type: 'string',
                    description: 'Optional StrataDB server base URL (default: http://127.0.0.1:8080 or STRATADB_URL env var).'
                }
            }
        }
    },
    {
        name: 'infeed_document',
        description: 'Infeeds an unstructured text document into a StrataDB vector collection. Automatically chunks text using sliding windows, generates 384-D semantic embeddings, and stores chunks in native database tables for RAG retrieval.',
        inputSchema: {
            type: 'object',
            properties: {
                collection: {
                    type: 'string',
                    description: 'Target collection name (e.g. "plant_manuals", "operating_procedures").'
                },
                title: {
                    type: 'string',
                    description: 'Document title or heading for source attribution.'
                },
                source: {
                    type: 'string',
                    description: 'Optional document identifier or reference code (e.g. "DOC-PUMP-01").'
                },
                text: {
                    type: 'string',
                    description: 'Unstructured text content to chunk, embed, and index.'
                },
                endpoint_url: {
                    type: 'string',
                    description: 'Optional StrataDB server base URL (default: http://127.0.0.1:8080 or STRATADB_URL env var).'
                }
            },
            required: ['collection', 'text']
        }
    },
    {
        name: 'infeed_table',
        description: 'Extracts relational table rows from StrataDB, concatenates selected columns into structured knowledge documents, generates 384-D dense embeddings, and stores them in a vector collection for RAG retrieval.',
        inputSchema: {
            type: 'object',
            properties: {
                collection: {
                    type: 'string',
                    description: 'Target collection name.'
                },
                table_name: {
                    type: 'string',
                    description: 'Name of the source relational table to infeed.'
                },
                columns: {
                    type: 'array',
                    items: { type: 'string' },
                    description: 'List of column names to extract and concatenate into knowledge representations.'
                },
                id_column: {
                    type: 'string',
                    description: 'Optional primary key column name for row identification.'
                },
                max_rows: {
                    type: 'integer',
                    description: 'Optional row limit to index (default: all rows, up to 1000).'
                },
                endpoint_url: {
                    type: 'string',
                    description: 'Optional StrataDB server base URL (default: http://127.0.0.1:8080 or STRATADB_URL env var).'
                }
            },
            required: ['collection', 'table_name', 'columns']
        }
    },
    {
        name: 'execute_sql',
        description: 'Executes a SQL query or command directly against StrataDB (e.g. SELECT, INSERT, UPDATE, CREATE TABLE, SHOW TABLES).',
        inputSchema: {
            type: 'object',
            properties: {
                sql: {
                    type: 'string',
                    description: 'SQL statement to execute.'
                },
                endpoint_url: {
                    type: 'string',
                    description: 'Optional StrataDB server base URL (default: http://127.0.0.1:8080 or STRATADB_URL env var).'
                }
            },
            required: ['sql']
        }
    }
];

// Tool Implementation Handlers
async function handleSearchKnowledgeBase(args) {
    const rawQuery = args.query || '';
    const query = String(rawQuery).trim();
    if (!query) {
        throw new Error('Missing required parameter: "query" must be a non-empty string.');
    }

    const rawTopK = Number(args.top_k);
    const topK = (!isNaN(rawTopK) && rawTopK > 0) ? Math.min(Math.max(Math.floor(rawTopK), 1), 20) : 3;

    const rawMinScore = Number(args.min_score);
    const minScore = (!isNaN(rawMinScore) && rawMinScore >= 0 && rawMinScore <= 1.0) ? rawMinScore : 0.2;

    const baseUrl = (args.endpoint_url || STRATADB_URL).replace(/\/+$/, '');

    // Determine collections
    let targetCollections = [];
    if (args.collection && typeof args.collection === 'string' && args.collection.trim().length > 0) {
        targetCollections = [args.collection.trim()];
    } else {
        try {
            const collResp = await fetch(`${baseUrl}/api/rag/collections`, {
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            });
            if (!collResp.ok) {
                const errText = await collResp.text().catch(() => '');
                throw new Error(`StrataDB responded with status ${collResp.status}: ${errText}`);
            }
            const collections = await collResp.json();
            if (!Array.isArray(collections) || collections.length === 0) {
                return {
                    status: 'empty',
                    query: query,
                    message: 'Knowledge base is currently empty. No vector collections have been indexed yet in StrataDB.',
                    available_collections: [],
                    matches: [],
                    context_prompt: ''
                };
            }
            targetCollections = collections.map(c => c.name);
        } catch (fetchErr) {
            if ((fetchErr.cause && fetchErr.cause.code === 'ECONNREFUSED') || fetchErr.message.includes('fetch failed')) {
                return {
                    status: 'error',
                    query: query,
                    message: `Could not connect to StrataDB RAG engine at ${baseUrl}. Ensure the StrataDB server daemon is running ('./zig-out/bin/stratadb --daemon').`,
                    error: fetchErr.message,
                    matches: []
                };
            }
            throw fetchErr;
        }
    }

    // Query collections
    const aggregatedMatches = [];
    const searchedCollections = [];
    const errors = [];

    for (const collName of targetCollections) {
        try {
            const queryResp = await fetch(`${baseUrl}/api/rag/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({
                    collection: collName,
                    query: query,
                    top_k: topK,
                    min_score: minScore
                })
            });

            if (!queryResp.ok) {
                const errBody = await queryResp.json().catch(() => null);
                const errMsg = errBody?.error || `HTTP ${queryResp.status}`;
                errors.push({ collection: collName, error: errMsg });
                continue;
            }

            const data = await queryResp.json();
            searchedCollections.push(collName);

            if (Array.isArray(data.matches)) {
                for (const match of data.matches) {
                    aggregatedMatches.push({
                        ...match,
                        collection: collName
                    });
                }
            }
        } catch (err) {
            errors.push({ collection: collName, error: err.message });
        }
    }

    // Sort descending by score and slice topK
    aggregatedMatches.sort((a, b) => (b.score || 0) - (a.score || 0));
    const topMatches = aggregatedMatches.slice(0, topK);

    // Format prompt context for LLM synthesis
    let contextPrompt = '';
    if (topMatches.length > 0) {
        const lines = [
            '=== RETRIEVAL-AUGMENTED GENERATION (RAG) CONTEXT ===',
            'You are provided the following verified excerpts retrieved from the StrataDB vector knowledge base.',
            'Use these excerpts to answer the user query accurately. Cite sources using [1], [2], etc.',
            ''
        ];

        topMatches.forEach((m, idx) => {
            const similarityPct = (m.score * 100).toFixed(1);
            const sourceTitle = m.doc_title || m.collection || 'Document';
            const chunkNum = m.chunk_index !== undefined ? m.chunk_index : idx;
            lines.push(`[${idx + 1}] Source: "${sourceTitle}" (Collection: ${m.collection}, Chunk #${chunkNum}, Similarity: ${similarityPct}%):`);
            lines.push(m.content || '');
            lines.push('');
        });

        lines.push('=== USER QUERY ===');
        lines.push(query);
        lines.push('==================================================');
        contextPrompt = lines.join('\n');
    }

    return {
        status: topMatches.length > 0 ? 'ok' : 'no_matches',
        query: query,
        collections_searched: searchedCollections,
        total_matches_found: topMatches.length,
        top_k: topK,
        min_score: minScore,
        matches: topMatches,
        context_prompt: contextPrompt,
        errors: errors.length > 0 ? errors : undefined,
        message: topMatches.length === 0
            ? `No excerpts matched the query with similarity >= ${(minScore * 100).toFixed(1)}% across collections: [${searchedCollections.join(', ')}].`
            : undefined
    };
}

async function handleListKnowledgeCollections(args) {
    const baseUrl = (args.endpoint_url || STRATADB_URL).replace(/\/+$/, '');
    const resp = await fetch(`${baseUrl}/api/rag/collections`, {
        method: 'GET',
        headers: { 'Accept': 'application/json' }
    });
    if (!resp.ok) {
        const errText = await resp.text().catch(() => '');
        throw new Error(`StrataDB responded with status ${resp.status}: ${errText}`);
    }
    return await resp.json();
}

async function handleInfeedDocument(args) {
    const baseUrl = (args.endpoint_url || STRATADB_URL).replace(/\/+$/, '');
    const resp = await fetch(`${baseUrl}/api/rag/infeed`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({
            collection: args.collection,
            title: args.title || 'Document',
            source: args.source || 'Manual Infeed',
            text: args.text
        })
    });
    if (!resp.ok) {
        const errBody = await resp.json().catch(() => null);
        throw new Error(errBody?.error || `HTTP ${resp.status}`);
    }
    return await resp.json();
}

async function handleInfeedTable(args) {
    const baseUrl = (args.endpoint_url || STRATADB_URL).replace(/\/+$/, '');
    const resp = await fetch(`${baseUrl}/api/rag/infeed/table`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({
            collection: args.collection,
            table_name: args.table_name,
            columns: args.columns,
            id_column: args.id_column || null,
            max_rows: args.max_rows || null
        })
    });
    if (!resp.ok) {
        const errBody = await resp.json().catch(() => null);
        throw new Error(errBody?.error || `HTTP ${resp.status}`);
    }
    return await resp.json();
}

async function handleExecuteSql(args) {
    const baseUrl = (args.endpoint_url || STRATADB_URL).replace(/\/+$/, '');
    const resp = await fetch(`${baseUrl}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({ sql: args.sql })
    });
    if (!resp.ok) {
        const errBody = await resp.json().catch(() => null);
        throw new Error(errBody?.error || `HTTP ${resp.status}`);
    }
    return await resp.json();
}

// Router for tool calls
async function callTool(name, args) {
    switch (name) {
        case 'search_knowledge_base':
            return await handleSearchKnowledgeBase(args || {});
        case 'list_knowledge_collections':
            return await handleListKnowledgeCollections(args || {});
        case 'infeed_document':
            return await handleInfeedDocument(args || {});
        case 'infeed_table':
            return await handleInfeedTable(args || {});
        case 'execute_sql':
            return await handleExecuteSql(args || {});
        default:
            throw new Error(`Unknown tool: ${name}`);
    }
}

// JSON-RPC 2.0 Dispatcher
function sendResponse(id, result) {
    const res = {
        jsonrpc: '2.0',
        id: id,
        result: result
    };
    process.stdout.write(JSON.stringify(res) + '\n');
}

function sendError(id, code, message) {
    const res = {
        jsonrpc: '2.0',
        id: id,
        error: {
            code: code,
            message: message
        }
    };
    process.stdout.write(JSON.stringify(res) + '\n');
}

async function handleRpcMessage(msg) {
    if (!msg || typeof msg !== 'object') return;

    const { id, method, params } = msg;

    // Handle notifications (no id)
    if (id === undefined || id === null) {
        if (method === 'notifications/initialized') {
            console.error('StrataDB MCP Server initialized and ready.');
        }
        return;
    }

    try {
        switch (method) {
            case 'initialize':
                sendResponse(id, {
                    protocolVersion: params?.protocolVersion || '2024-11-05',
                    capabilities: {
                        tools: {}
                    },
                    serverInfo: {
                        name: 'stratadb-mcp',
                        version: '1.0.0'
                    }
                });
                break;

            case 'ping':
                sendResponse(id, {});
                break;

            case 'tools/list':
                sendResponse(id, {
                    tools: TOOLS
                });
                break;

            case 'tools/call': {
                const toolName = params?.name;
                const toolArgs = params?.arguments || {};
                const toolResult = await callTool(toolName, toolArgs);
                sendResponse(id, {
                    content: [
                        {
                            type: 'text',
                            text: typeof toolResult === 'string' ? toolResult : JSON.stringify(toolResult, null, 2)
                        }
                    ]
                });
                break;
            }

            default:
                sendError(id, -32601, `Method not found: ${method}`);
                break;
        }
    } catch (err) {
        if (method === 'tools/call') {
            sendResponse(id, {
                isError: true,
                content: [
                    {
                        type: 'text',
                        text: `Error executing ${params?.name}: ${err.message}`
                    }
                ]
            });
        } else {
            sendError(id, -32603, err.message);
        }
    }
}

// Stdio Line Reader
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    try {
        const msg = JSON.parse(trimmed);
        handleRpcMessage(msg);
    } catch (err) {
        console.error('Failed to parse incoming JSON-RPC line:', err.message);
    }
});

console.error(`StrataDB MCP Server running on Stdio (targeting ${STRATADB_URL})`);
