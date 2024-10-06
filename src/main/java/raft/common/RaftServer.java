package raft.common;

import raft.network.MessageStatus;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public abstract class RaftServer extends Node<RaftMessage> {

    private ServerSocketChannel serverSocketChannel;
    private ConcurrentHashMap<Node, SocketConnection> connections;
    private List<Node<RaftMessage>> clients;
    private List<Node<RaftMessage>> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private Thread messageSendingThread;
    private final ConcurrentHashMap<Node<RaftMessage>, Queue<RaftMessage>> outgoingMessages;
    private final ConcurrentLinkedQueue<RaftMessage> incomingMessages;
    private Selector incomingMessageSelector;
    private List<SelectionKey> channelSelectionKeys;

    public RaftServer(InetSocketAddress address) throws IOException {
        super(address);
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(address);
        acceptingConnections = new AtomicBoolean();
        acceptingConnections.set(false);

        clients = new LinkedList<>();
        servers = new LinkedList<>();

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
        runRaft();
    }

    private SocketConnection acceptConnection() throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        SocketConnection connection = new SocketConnection(
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
                    // Initialize connection details
                    SocketConnection connection = acceptConnection();
                    String remoteIP = connection.getNonBlockingChannel().socket().getInetAddress().getHostAddress();
                    Integer remotePort = connection.getNonBlockingChannel().socket().getPort();
                    // Store endpoint
                    InetSocketAddress remoteAddress = new InetSocketAddress(remoteIP, remotePort);
                    connection.endpoint = new Node<RaftMessage>(remoteAddress);

                    // Store connection for reuse
                    connections.put(connection.endpoint, connection);
                    clients.add(connection.endpoint);

                    // Register with incoming message listener
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

    public Collection<SocketConnection> getConnections() {
        return connections.values();
    }

    public void queueMessage (RaftMessage message, Node<RaftMessage> node) {
        synchronized (outgoingMessages) {
            if (node == null) {
                System.out.println("AAAA");
            }
            Queue<RaftMessage> messageQueue = outgoingMessages.getOrDefault(node, new ConcurrentLinkedQueue<>());
            messageQueue.add(message);
            outgoingMessages.put(node, messageQueue);
            outgoingMessages.notifyAll();
        }
//        System.out.println(Thread.currentThread().getName() + "\tQueued message " + message + " for " + node.getInetSocketAddress());
    }

    public void queueMessage (RaftMessage message, List<Node<RaftMessage>> nodes) {
        for (Node<RaftMessage> node : nodes) {
            queueMessage(message, node);
        }
    }

    public void queueServerBroadcast (RaftMessage message) {
        queueMessage(message, servers);
    }
    public void queueClientBroadcast (RaftMessage message) {
        queueMessage(message, clients);
    }

    private void acceptOneMessage(SocketConnection connection) {
        RaftMessage message = connection.receive();
        synchronized (incomingMessages) {
            message.setSender(connection.endpoint);
            incomingMessages.add(message);
            incomingMessages.notifyAll();
        }
    }

    private void startAcceptingMessages() {
        messageReceivingThread = new Thread(() -> {
            while (true) {
                try {
                    incomingMessageSelector.select();
                    incomingMessageSelector.selectedKeys()
                            .stream()
                            .map((SelectionKey k) -> (SocketConnection) k.attachment())
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
        return outgoingMessages.values().stream().anyMatch(new Predicate<Queue<RaftMessage>>() {
            @Override
        public boolean test(Queue<RaftMessage> raftMessages) {
                return !raftMessages.isEmpty();
            }
        });
    }

    private void startSendingMessages() {
        messageSendingThread = new Thread(() -> {
            while (true) {
                while (!hasReadyMessages()) {
                    try {
                        synchronized (outgoingMessages) {
                            outgoingMessages.wait(100);
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }
                }

                outgoingMessages.forEach((Node<RaftMessage> node, Queue<RaftMessage> messages) ->
                        messages.forEach((RaftMessage msg) -> {
                            if (msg.getStatus() == MessageStatus.READY) {
                                connections.get(node).send(msg);
                                msg.setStatus(MessageStatus.SENT);
                            }
                        }));
            }
        });

        messageSendingThread.setDaemon(true);
        messageSendingThread.start();
    }

    public abstract void runRaft();
}
