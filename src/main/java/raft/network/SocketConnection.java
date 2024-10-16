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
import raft.common.Colors;
import raft.messaging.common.RaftMessage;


public class SocketConnection implements Connection <RaftMessage>, AutoCloseable {
    protected SocketChannel socketChannel;

    public Node<RaftMessage> endpoint;

    private ObjectMapper mapper;

    public SocketConnection (String remoteAddress, Integer remotePort, SocketChannel socketChannel) throws IOException {
        this.endpoint = new Node<>(new InetSocketAddress(remoteAddress, remotePort));
        this.socketChannel = socketChannel;

        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    public SocketConnection (String remoteAddress, Integer remotePort) throws IOException {
        this(remoteAddress,
                remotePort,
                SocketChannel.open(new InetSocketAddress(remoteAddress, remotePort)));
    }

    @Override
    public boolean send(RaftMessage value) {
        try {
            byte[] data = mapper.writeValueAsBytes(value);

            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + data.length);
            buf.putLong(data.length);
            buf.put(data);
//            System.out.printf("%s sending: %s\n", Thread.currentThread().getName(), new String(data));

            buf.flip();
            if (socketChannel.write(buf) < buf.limit()) {
                System.out.printf(Colors.GREEN + "[!] %s: Write incomplete!\n" + Colors.RESET, Thread.currentThread().getName());
            }
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
            // Read data size
            ByteBuffer sizeBuf = ByteBuffer.allocate(Long.BYTES);
            while (sizeBuf.position() < sizeBuf.capacity()) {
                int result = socketChannel.read(sizeBuf);
                if (result == -1) return null;
                else if (result < sizeBuf.limit()) {
                    System.out.printf(Colors.GREEN + "[!] %s: Header read incomplete (read: %d, needed: %d)!\n" + Colors.RESET, Thread.currentThread().getName(), result, sizeBuf.limit());
                    try {
                        Thread.sleep(20);
                    }
                    catch (InterruptedException e) {
                        continue;
                    }
                }
            }
            sizeBuf.flip();
            int dataSize = (int)sizeBuf.asLongBuffer().get();

            // Read data
            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            while (dataBuf.position() < dataBuf.capacity()) {
                int result = socketChannel.read(dataBuf);
                if (result == -1) return null;
                else if (result < dataBuf.limit()) {
                    System.out.printf(Colors.GREEN + "[!] %s: Body read incomplete!\n" + Colors.RESET, Thread.currentThread().getName());
                    try {
                        Thread.sleep(5);
                    }
                    catch (InterruptedException e) {
                        continue;
                    }
                }
            }
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

    @Override
    public String toString() {
        return String.format("SocketConnection to %s", endpoint);
    }
}
