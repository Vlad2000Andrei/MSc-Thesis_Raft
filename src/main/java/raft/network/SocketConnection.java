package raft.network;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class SocketConnection <T extends Serializable> implements Connection <T>, AutoCloseable {
    private SocketChannel socketChannel;
    private String remoteAddress;
    private Integer remotePort;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;

    public SocketConnection (String remoteAddress, Integer remotePort, SocketChannel socketChannel) throws IOException {
        this.remoteAddress = remoteAddress;
        this.remotePort = remotePort;
        this.socketChannel = socketChannel;
        outputStream = new ObjectOutputStream(socketChannel.socket().getOutputStream());
        inputStream = new ObjectInputStream(socketChannel.socket().getInputStream());
    }

    public SocketConnection (String remoteAddress, Integer remotePort) throws IOException {
        this(remoteAddress,
                remotePort,
                SocketChannel.open(new InetSocketAddress(remoteAddress, remotePort)));
    }

    @Override
    public boolean send(T value) {
        try {
            // TODO: Fix with https://stackoverflow.com/questions/5638395/sending-objects-through-java-nio-non-blocking-sockets
            outputStream.writeObject(value);
            System.out.println("[i] Sent: " + value + " to " + socketChannel.socket().getInetAddress());
            return true;
        }
        catch (IOException e) {
            System.out.println("[ERR] Could not write to socket connection.");
            return false;
        }
    }

    @Override
    public T receive() {
        try {
            System.out.println("Here");
            T result = (T) inputStream.readObject();
            System.out.println("[i] Received: " + result + " from " + socketChannel.socket().getInetAddress());
            return result;
        }
        catch (Exception e) {
            System.out.println("[ERR] Could not write to socket connection.");
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (!socketChannel.socket().isClosed()) socketChannel.close();
    }

    public boolean isClosed() {
        return socketChannel.socket().isClosed();
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public Integer getRemotePort() {
        return remotePort;
    }

    public SocketChannel getNonBlockingChannel() throws IOException {
        socketChannel.configureBlocking(false);
        return socketChannel;
    }
}
