package raft.messaging.common;

public enum ControlMessageType {
    HELLO_SERVER,
    HELLO_CLIENT,
    GET_LOG,
    LOG,
    STOP
}
