package raft.network;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Locale;

import com.fasterxml.jackson.core.json.async.NonBlockingByteBufferJsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.std.ByteBufferSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import raft.common.RaftMessage;
import raft.common.RaftServer;


public class SocketConnection implements Connection <RaftMessage>, AutoCloseable {
    private SocketChannel socketChannel;

    public Node<RaftMessage> endpoint;

    public SocketConnection (String remoteAddress, Integer remotePort, SocketChannel socketChannel) throws IOException {
        this.endpoint = new Node<RaftMessage>(new InetSocketAddress(remoteAddress, remotePort));
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

            buf.flip();
            socketChannel.write(buf);
//            System.out.println(Thread.currentThread().getName() + " \tsent to " + endpoint.getInetSocketAddress() + "\t\t" + Instant.now());


            return true;
        }
        catch (IOException e) {
            System.out.println("[ERR] Could not write to socket connection.");
            return false;
        }
    }

    @Override
    public RaftMessage receive() {
        try {
//            System.out.println(Thread.currentThread().getName() + " \tWaiting for receive() from " + endpoint.getInetSocketAddress() + "\t\t" + Instant.now());
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());

            // Read data size
            ByteBuffer sizeBuf = ByteBuffer.allocate(Long.BYTES);
            socketChannel.read(sizeBuf);
            sizeBuf.flip();
            int dataSize = (int)sizeBuf.asLongBuffer().get();

            // Read data
            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            socketChannel.read(dataBuf);
            dataBuf.flip();

            // Map back to T
            RaftMessage result = mapper.readValue(dataBuf.array(), RaftMessage.class);
            return result;
        }
        catch (Exception e) {
            System.out.println("[ERR] Could not read from connection " + endpoint.getInetSocketAddress());
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
