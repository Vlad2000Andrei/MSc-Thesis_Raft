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
        return String.format("[AppendEntries] Term: %d \tLeader ID: %d \tPrevious Entry: idx %d in term %d \tLast Committed: %d \tEntries: %s. \t Timestamp: %s",
                term, id, prevLogIdx, prevLogTerm, leaderCommit, entries, Instant.now());
    }
}
