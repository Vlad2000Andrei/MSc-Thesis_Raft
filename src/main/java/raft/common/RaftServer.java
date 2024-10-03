package raft.common;

import raft.network.Connection;
import raft.network.MessageStatus;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RaftServer extends Node<RaftMessage> {

    private ServerSocketChannel serverSocketChannel;
    private ConcurrentHashMap<Node, SocketConnection<RaftMessage>> connections;
    private List<Node> clients;
    private List<Node> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private Thread messageSendingThread;
    private final Map<Node<RaftMessage>, List<RaftMessage>> outgoingMessages;
    private final ConcurrentLinkedQueue<RaftMessage> incomingMessages;
    private Selector incomingMessageSelector;
    private List<SelectionKey> channelSelectionKeys;

    public RaftServer(InetSocketAddress address) throws IOException {
        super(address);
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(address);
        acceptingConnections = new AtomicBoolean();
        acceptingConnections.set(false);

        connections = new ConcurrentHashMap<>();
        outgoingMessages = new ConcurrentHashMap<>();
        incomingMessages = new ConcurrentLinkedQueue<>();
        incomingMessageSelector = Selector.open();

        channelSelectionKeys = new ArrayList<>();
    }

    public void start() {
        acceptConnections();
        startAcceptingMessages();
        startSendingMessages();
    }

    private SocketConnection<RaftMessage> acceptConnection() throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        SocketConnection<RaftMessage> connection = new SocketConnection<RaftMessage>(
                Arrays.toString(socketChannel.socket().getInetAddress().getAddress()),
                socketChannel.socket().getPort(),
                socketChannel
        );
        return connection;
    }

    private void acceptConnections() {
        acceptingConnections.set(true);
        Runnable acceptLoop = () -> {
            while (acceptingConnections.get()) {
                try {
                    SocketConnection<RaftMessage> connection = acceptConnection();
                    InetSocketAddress remoteAddress = new InetSocketAddress(connection.getRemoteAddress(), connection.getRemotePort());
                    Node<RaftMessage> remoteNode = new Node<>(remoteAddress);

                    connections.put(remoteNode, connection);
                    SelectionKey selectionKey = connection.getNonBlockingChannel().register(incomingMessageSelector, SelectionKey.OP_READ);
                    selectionKey.attach(connection);
                    channelSelectionKeys.add(selectionKey);
                    incomingMessageSelector.wakeup();

                } catch (IOException e) {
                    System.out.println("[ERR] Failed to accept connection: " + e.getMessage());

                    if (!serverSocketChannel.socket().isBound()) {
                        acceptingConnections.set(false);
                        break;
                    }

                    if (serverSocketChannel.socket().isClosed()) {
                        acceptingConnections.set(false);
                        break;
                    }
                }
            }
        };

        acceptingConnectionsThread = new Thread(acceptLoop);
        acceptingConnectionsThread.setDaemon(true);
        acceptingConnectionsThread.start();
    }

    public Collection<SocketConnection<RaftMessage>> getConnections() {
        return connections.values();
    }

    public void queueMessage (RaftMessage message, Node<RaftMessage> node) {
        List<RaftMessage> messageQueue =  outgoingMessages.getOrDefault(node, new ArrayList<>());
        messageQueue.add(message);
        outgoingMessages.put(node, messageQueue);
        outgoingMessages.notifyAll();
    }

    public void queueMessage (RaftMessage message, List<Node<RaftMessage>> nodes) {
        for (Node<RaftMessage> node : nodes) {
            queueMessage(message, node);
        }
    }

    private void acceptOneMessage(SocketConnection<RaftMessage> connection) {
        RaftMessage message = connection.receive();
        System.out.println("[i] Accepted message: " + message);
        incomingMessages.add(message);
        incomingMessages.notifyAll();
    }

    private void startAcceptingMessages() {
        messageReceivingThread = new Thread(() -> {
            while (true) {
                try {
                    incomingMessageSelector.select();
                    incomingMessageSelector.selectedKeys()
                            .stream()
                            .map((SelectionKey k) -> (SocketConnection<RaftMessage>) k.attachment())
                            .forEach(this::acceptOneMessage);
                } catch (IOException e) {
                    System.out.println("[ERR Could not select incoming messages: " + e.getMessage());
                }
            }
        });

        messageReceivingThread.setDaemon(true);
        messageReceivingThread.start();
    }

    public RaftMessage getNextMessage() {
        synchronized (incomingMessages) {
            while (incomingMessages.isEmpty()) {
                try {
                    incomingMessages.wait();
                }
                catch (InterruptedException e) {
                    continue;
                }
            }
        }
        return incomingMessages.remove();
    }

    private boolean hasReadyMessages() {
        return outgoingMessages.values().stream().anyMatch(new Predicate<List<RaftMessage>>() {
            @Override
            public boolean test(List<RaftMessage> raftMessages) {
                return !raftMessages.isEmpty();
            }
        });
    }

    private void startSendingMessages() {
        messageSendingThread = new Thread(() -> {
            while (!hasReadyMessages()) {
                try {
                    synchronized (outgoingMessages) {
                        outgoingMessages.wait();
                    }
                } catch (InterruptedException e) {
                    continue;
                }
            }

            outgoingMessages.forEach((Node<RaftMessage> node, List<RaftMessage> messages) -> {
                messages.stream()
                        .filter((RaftMessage msg) -> msg.getStatus() == MessageStatus.READY)
                        .forEach((RaftMessage msg) -> {
                            connections.get(node).send(msg);
                            msg.setStatus(MessageStatus.SENT);
                        });
            });
        });

        messageSendingThread.setDaemon(true);
        messageSendingThread.start();
    }
}
