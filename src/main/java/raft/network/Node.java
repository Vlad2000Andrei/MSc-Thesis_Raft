package raft.network;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Node<T extends Serializable> {

    private InetSocketAddress inetSocketAddress;

    public Node (InetSocketAddress address) {
        inetSocketAddress = address;
    }

    public SocketConnection<T> connectTo(Node<T> node) throws IOException {
        InetSocketAddress nodeAddress = node.getInetSocketAddress();
        SocketConnection<T> connection = new SocketConnection<T>(nodeAddress.getAddress().getHostAddress(), nodeAddress.getPort());
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
        return Objects.hash(inetSocketAddress);
    }
}
