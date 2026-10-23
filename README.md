# StrataDB: Distributed Relational Database for Connected Desktops

StrataDB is a lightweight, high-performance distributed relational database written in pure **Zig** (zero external C dependencies). Designed specifically for desktop workstations and laptops, StrataDB functions as an embedded local relational engine that connects peer-to-peer across LANs to automatically synchronize tables and resolve concurrent edits with mathematical convergence.

---

## Architecture Overview

```
 ┌──────────────────────────────────────────────────────────────┐
 │                      Desktop User Interface                  │
 │   ┌───────────────────────────┐   ┌──────────────────────┐   │
 │   │  Interactive REPL Shell   │   │ Modern Web Dashboard │   │
 │   │     (stratadb CLI)        │   │ (http://localhost)   │   │
 │   └─────────────┬─────────────┘   └──────────┬───────────┘   │
 └─────────────────┼────────────────────────────┼───────────────┘
                   │                            │
                   ▼                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │               SQL Parser & Query Execution Engine            │
 │   - DDL: CREATE TABLE, DROP TABLE, SHOW TABLES, DESCRIBE     │
 │   - DML: INSERT, SELECT, UPDATE, DELETE                      │
 │   - Relational: WHERE (and/or/not), ORDER BY, LIMIT, JOIN    │
 │   - Aggregates: COUNT(*), SUM, AVG, MIN, MAX                 │
 └──────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
 ┌──────────────────────────────────────────────────────────────┐
 │                 Storage & Durability Subsystem               │
 │   - Primary-Key Row Store with Tombstones                    │
 │   - Write-Ahead Log (WAL) with LSN & sync checkpoints        │
 │   - Hybrid Logical Clock (HLC) per-row version stamps        │
 └──────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
 ┌──────────────────────────────────────────────────────────────┐
 │                 P2P Multi-Desktop Sync Engine                │
 │   ┌────────────────────────────────────────────────────────┐ │
 │   │ Change Data Capture (CDC) Monotonic Mutation Log       │ │
 │   ├────────────────────────────────────────────────────────┤ │
 │   │ LWW-CRDT Conflict Resolver (Causal HLC + UUID Tiebreak)│ │
 │   ├────────────────────────────────────────────────────────┤ │
 │   │ Binary TCP Replication Protocol (Delta Sync + Streams) │ │
 │   ├────────────────────────────────────────────────────────┤ │
 │   │ UDP Zero-Configuration LAN Peer Discovery (Beacon)     │ │
 │   └────────────────────────────────────────────────────────┘ │
 └──────────────────────────────────────────────────────────────┘
```

---

## Key Highlights

1. **Pure Zig Implementation**: Built without SQLite or C dependencies. Runs as a single self-contained native binary (< 2 MB).
2. **Hybrid Logical Clocks (HLC)**: Combines physical wall-clock time with logical sequence counters and node UUIDs. Avoids wall-clock drift, NTP jumps, and causality violations across desktops.
3. **LWW-CRDT Multi-Master Convergence**: Tables act as Last-Write-Wins Conflict-Free Replicated Data Types. Desktops can edit data while offline; upon reconnecting, changes converge deterministically across all peers.
4. **Change Data Capture (CDC)**: Every mutation (`INSERT`, `UPDATE`, `DELETE`) is captured with monotonic sequence numbers, cryptographic node IDs, and row tombstones.
5. **Zero-Configuration LAN Auto-Discovery**: Nodes broadcast UDP heartbeat beacons (`SDIS` protocol) to automatically discover peer desktops on the local network.
6. **Built-in Desktop Web Console & REST API**: Live dashboard at `http://localhost:8080` featuring node metrics, active peers, and an interactive SQL query runner.

---

## Quick Start

### 1. Build the Binary

```bash
# Build with Zig 0.13.0
zig build-exe src/main.zig -O ReleaseSafe -femit-bin=zig-out/bin/stratadb
```

### 2. Start Desktop Node 1 (MacBook)

```bash
./zig-out/bin/stratadb --name "MacBook" --port 7433 --web-port 8080 --data ./data_mac
```

Visit the desktop web console: **[http://localhost:8080](http://localhost:8080)**.

### 3. Start Desktop Node 2 (iMac / PC)

Open a second terminal window:

```bash
./zig-out/bin/stratadb --name "iMac" --port 7434 --web-port 8081 --data ./data_imac --connect 127.0.0.1:7433
```

Visit the second desktop's web console: **[http://localhost:8081](http://localhost:8081)**.

---

## Linking Connected Desktops (Auto-Sync)

### Automatic LAN Discovery
By default, each node broadcasts a UDP beacon on port `7432`. Desktops on the same Wi-Fi / Ethernet subnet automatically detect each other and initiate TCP replication.

### Manual Direct Connect
From the SQL CLI or dashboard:
```sql
SYNC CONNECT 192.168.1.45:7433;
```
Or view connected desktop peers:
```sql
PEERS;
```

---

## SQL Dialect Guide

### Table Creation & Management
```sql
CREATE TABLE tasks (
    id INT PRIMARY KEY,
    title TEXT,
    priority INT,
    completed BOOL
);

SHOW TABLES;
DESCRIBE tasks;
```

### Data Manipulation
```sql
INSERT INTO tasks VALUES (1, 'Finish project proposal', 1, false);
INSERT INTO tasks VALUES (2, 'Review sync logs', 2, true);
INSERT INTO tasks VALUES (3, 'Benchmark disk engine', 1, false);

UPDATE tasks SET completed = true WHERE id = 1;
DELETE FROM tasks WHERE id = 2;
```

### Queries, Filtering & Aggregation
```sql
-- Filter and order
SELECT id, title, completed FROM tasks WHERE completed = false ORDER BY priority ASC;

-- Aggregations
SELECT COUNT(*), AVG(priority), MAX(priority) FROM tasks;
```

---

## Desktop Web Dashboard & REST API

### Web Management UI
Navigate to `http://localhost:8080`:
- **Node Metrics**: Current node ID, desktop hostname, and physical/logical HLC timestamp.
- **Peer List**: Real-time connected desktop nodes.
- **SQL Console**: Execute ad-hoc queries with instant HTML table rendering.

### REST Endpoints
- `GET /api/status`: Node info, table count, CDC log count, HLC timestamp.
- `GET /api/peers`: Array of connected peer desktops.
- `POST /api/query`: JSON payload `{"query": "SELECT * FROM tasks;"}` returning JSON columns and rows.

---

## Running the Verification Test Suite

Verify relational SQL execution and multi-desktop CRDT convergence:

```bash
zig test src/sync_simulation_test.zig
```

All tests execute with **0 leaks** verified under Zig's General Purpose Allocator.
