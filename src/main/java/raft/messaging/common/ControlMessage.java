package raft.messaging.common;

public record ControlMessage(
    ControlMessageType type,
    int term,
    boolean result, // success or failure
    long resultOf // sequence nr of matching request
) {}
