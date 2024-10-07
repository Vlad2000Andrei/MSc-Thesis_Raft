package raft.messaging.common;

public record ControlMessage(
    ControlMessageType type,
    boolean result, // success or failure
    int resultOf // sequence nr of matching request
) {}
