package raft.network;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import raft.messaging.common.RaftMessage;


public class SocketConnection implements Connection <RaftMessage>, AutoCloseable {
    private SocketChannel socketChannel;

    public Node<RaftMessage> endpoint;

    public SocketConnection (String remoteAddress, Integer remotePort, SocketChannel socketChannel) throws IOException {
        this.endpoint = new Node<>(new InetSocketAddress(remoteAddress, remotePort));
        this.socketChannel = socketChannel;
    }

    public SocketConnection (String remoteAddress, Integer remotePort) throws IOException {
        this(remoteAddress,
                remotePort,
                SocketChannel.open(new InetSocketAddress(remoteAddress, remotePort)));
    }

    @Override
    public boolean send(RaftMessage value) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());

            byte[] data = mapper.writeValueAsBytes(value);

            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + data.length);
            buf.putLong(data.length);
            buf.put(data);
//            System.out.printf("%s sending: %s\n", Thread.currentThread().getName(), new String(data));

            buf.flip();
            socketChannel.write(buf);
            return true;
        }
        catch (IOException e) {
            System.out.println("[ERR] Could not write to socket connection. (" + Thread.currentThread().getName() + ")");
            return false;
        }
    }

    @Override
    public RaftMessage receive() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());

            // Read data size
            ByteBuffer sizeBuf = ByteBuffer.allocate(Long.BYTES);
            while (sizeBuf.position() < sizeBuf.capacity()) {
                if (socketChannel.read(sizeBuf) == -1) return null;
            }
            sizeBuf.flip();
            int dataSize = (int)sizeBuf.asLongBuffer().get();

            // Read data
            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            while (dataBuf.position() < dataBuf.capacity()) {
                if (socketChannel.read(dataBuf) == -1) return null;
            }
//            System.out.printf("%s received: %s\n", Thread.currentThread().getName(), new String(dataBuf.array()));
            dataBuf.flip();

            // Map back to T
            return mapper.readValue(dataBuf.array(), RaftMessage.class);
        }
        catch (IOException e) {
            System.out.printf("[ERR] Could not read from connection %s (%s)\n", endpoint.getInetSocketAddress(), Thread.currentThread().getName());
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
        return endpoint.getInetSocketAddress().getAddress().getHostAddress();
    }

    public Integer getRemotePort() {
        return endpoint.getInetSocketAddress().getPort();
    }

    public SocketChannel getNonBlockingChannel() throws IOException {
        socketChannel.configureBlocking(false);
        return socketChannel;
    }
}
