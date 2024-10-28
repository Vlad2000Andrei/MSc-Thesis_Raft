package raft.messaging.internal;

public enum VoteResponseStatus {
    ACCEPT,
    REJECT,
    CANCEL,
    ACK_CANCEL
}
