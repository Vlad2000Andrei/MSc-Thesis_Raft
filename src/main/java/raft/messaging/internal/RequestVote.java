package raft.messaging.internal;

public record RequestVote(
        int term,
        int candidateId,
        int lastLogIndex,
        int lastLogTerm
) {}
