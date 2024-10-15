package raft.network;

import raft.messaging.common.RaftMessage;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Node<T extends Serializable> {

    private final InetSocketAddress inetSocketAddress;
    public Integer id;

    public Node (InetSocketAddress address) {
        inetSocketAddress = address;
    }

    public SocketConnection connectTo(Node<T> node) throws IOException {
        InetSocketAddress nodeAddress = node.getInetSocketAddress();
        SocketConnection connection = new SocketConnection(nodeAddress.getAddress().getHostAddress(), nodeAddress.getPort());

        return connection;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node<?> node)) return false;
        return Objects.equals(inetSocketAddress, node.inetSocketAddress);
    }

    @Override
    public int hashCode() {
        return inetSocketAddress.hashCode();
    }

    @Override
    public String toString() {
        return "Node at " + getInetSocketAddress().toString();
    }
}
