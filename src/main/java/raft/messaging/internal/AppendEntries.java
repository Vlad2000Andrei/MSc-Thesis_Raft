package raft.messaging.internal;

import raft.common.LogEntry;

import java.time.Instant;
import java.util.List;

public record AppendEntries(
        int term,
        int id,
        int prevLogIdx,
        int prevLogTerm,
        List<LogEntry> entries,
        int leaderCommit
) {
    @Override
    public String toString() {
        return String.format("[AppendEntries] Term: %d Leader ID: %d Previous Entry: idx %d in term %d Last Committed: %d Entries: %s.",
                term, id, prevLogIdx, prevLogTerm, leaderCommit, entries);
    }
}
