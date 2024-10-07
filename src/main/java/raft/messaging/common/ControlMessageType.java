package raft.messaging.common;

public enum ControlMessageType {
    HELLO_SERVER,
    HELLO_CLIENT,
    APPEND_ENTRIES_RESULT,
    REQUEST_VOTE_RESULT
}
