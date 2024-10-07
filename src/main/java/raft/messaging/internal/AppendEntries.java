package raft.messaging.internal;

import raft.common.LogEntry;

import java.util.List;

public record AppendEntries(
        int term,
        int id,
        int prevLogId,
        int prevLogTerm,
        LogEntry[] entries,
        int leaderCommit
) {}
