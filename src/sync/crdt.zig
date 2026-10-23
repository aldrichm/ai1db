const std = @import("std");
const types = @import("../types.zig");
const Value = types.Value;
const Row = types.Row;
const HLC = @import("hlc.zig").HLC;
const cdc = @import("cdc.zig");
const ChangeRecord = cdc.ChangeRecord;
const Operation = cdc.Operation;

pub const ConflictResolution = enum {
    applied_incoming,
    retained_local,
    identical_noop,
};

pub const RowState = struct {
    hlc: HLC,
    is_deleted: bool,
    row: ?Row,
};

/// Evaluates whether an incoming ChangeRecord should be applied over the existing local state.
/// Implements Last-Write-Wins (LWW) CRDT semantics using Hybrid Logical Clocks.
pub fn resolveChange(
    local_state: ?RowState,
    incoming: ChangeRecord,
) ConflictResolution {
    if (local_state == null) {
        // No local record exists; incoming is applied immediately
        return .applied_incoming;
    }

    const local = local_state.?;
    const cmp = incoming.hlc.compare(local.hlc);

    switch (cmp) {
        .gt => return .applied_incoming, // Incoming is newer in causal HLC order
        .lt => return .retained_local,   // Incoming is older; local is newer
        .eq => {
            // Clocks are strictly equal (same physical time, logical count, node id)
            return .identical_noop;
        },
    }
}
