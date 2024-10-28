package raft.messaging.internal;

public record RequestVoteResponse(
        VoteResponseStatus result,
        int term
) {
}
