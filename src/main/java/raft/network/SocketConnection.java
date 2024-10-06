package raft.network;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
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
            System.out.println("[ERR] Could not write to socket connection. (" + Thread.currentThread().getName() + ")");
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
            dataBuf.flip();

            // Map back to T
            return mapper.readValue(dataBuf.array(), RaftMessage.class);
        }
        catch (IOException e) {
            System.out.println("[ERR] Could not read from connection " + endpoint.getInetSocketAddress() + " (" + Thread.currentThread().getName() + ")");
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
