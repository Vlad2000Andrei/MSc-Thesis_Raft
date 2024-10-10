package raft.messaging.internal;

import raft.common.LogEntry;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public record AppendEntries(
        int term,
        int id,
        int prevLogId,
        int prevLogTerm,
        List<LogEntry> entries,
        int leaderCommit
) {
    @Override
    public String toString() {
        return String.format("[AppendEntries] Term: %d \tLeader ID: %d \tPrevious Entry: leader %d in term %d \tLast Committed: %d \tEntries: %s. \t Timestamp: %s",
                term, id, prevLogId, prevLogTerm, leaderCommit, entries, Instant.now());
    }
}
