package raft.messaging.internal;

public record AppendEntriesResponse(
    int term,
    boolean result,
    int entryIndex
) {
}
