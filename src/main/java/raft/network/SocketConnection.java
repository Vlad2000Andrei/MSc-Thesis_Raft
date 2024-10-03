package raft.network;

import raft.common.RaftMessage;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;

public class SocketConnection <T extends Serializable> implements Connection <T>, AutoCloseable {
    private Socket socket;
    private String remoteAddress;
    private Integer remotePort;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;

    public SocketConnection (String remoteAddress, Integer remotePort, Socket socket) throws IOException {
        this.remoteAddress = remoteAddress;
        this.remotePort = remotePort;
        this.socket = socket;
        outputStream = new ObjectOutputStream(socket.getOutputStream());
        inputStream = new ObjectInputStream(socket.getInputStream());
    }

    public SocketConnection (String remoteAddress, Integer remotePort) throws IOException {
        this(remoteAddress, remotePort, new Socket(remoteAddress, remotePort));
    }

    @Override
    public boolean send(T value) {
        try {
            outputStream.writeObject(value);
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
            T result = (T) inputStream.readObject();
            return result;
        }
        catch (Exception e) {
            System.out.println("[ERR] Could not write to socket connection.");
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (!socket.isClosed()) socket.close();
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public Integer getRemotePort() {
        return remotePort;
    }

    public SocketChannel getNonBlockingChannel() throws IOException {
        SocketChannel channel = socket.getChannel();
        channel.configureBlocking(false);
        return channel;
    }
}
