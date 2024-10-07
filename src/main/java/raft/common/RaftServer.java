package raft.common;

import org.jetbrains.annotations.NotNull;
import raft.messaging.common.MessageStatus;
import raft.messaging.common.RaftMessage;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public abstract class RaftServer extends Node<RaftMessage> {

    public final int sequenceNr;
    private static AtomicInteger nextSequenceNr = new AtomicInteger(0);
    private ServerSocketChannel serverSocketChannel;
    private ConcurrentHashMap<Node, SocketConnection> connections;
    private List<Node<RaftMessage>> clients;
    private List<Node<RaftMessage>> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private Thread messageSendingThread;
    private final ConcurrentHashMap<Node<RaftMessage>, Queue<RaftMessage>> outgoingMessages;
    private final Timer timeoutTimer;
    private final ConcurrentLinkedQueue<RaftMessage> incomingMessages;
    private Selector incomingMessageSelector;
    private List<SelectionKey> channelSelectionKeys;

    public RaftServer(InetSocketAddress address) throws IOException {
        super(address);
        sequenceNr = nextSequenceNr.getAndIncrement();
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
        timeoutTimer = new Timer("Timeout Timer");
    }

    public void start() {
        acceptConnections();
        startAcceptingMessages();
        startSendingMessages();
        runRaft();
    }

    private void discardConnection (SocketConnection connection) {
        try {
            System.out.printf("[i] Discarding connection to %s : %s. Remaining connections: %s.\n",
                    connection.getNonBlockingChannel().socket().getInetAddress().getHostAddress(),
                    connection.getNonBlockingChannel().socket().getPort(),
                    connections.size() - 1);

            clients.remove(connection.endpoint);
            servers.remove(connection.endpoint);
            outgoingMessages.remove(connection.endpoint);
            connections.remove(connection.endpoint);
            connection.getNonBlockingChannel().close();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
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
                    connection.endpoint = new Node<>(remoteAddress);
                    System.out.printf("[i] (%s) Client (%s) connected on local port %s.\n",
                            Thread.currentThread().getName(),
                            remoteAddress,
                            connection.getNonBlockingChannel().socket().getLocalPort());

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

    public void queueMessage (RaftMessage message, @NotNull Node<RaftMessage> node) {
        synchronized (outgoingMessages) {
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

            if (message.getTimeout() != null) {
                scheduleMessageTimeout(message);
            }
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
        if (message == null) {
            discardConnection(connection);
            return;
        }
        message.setSender(connection.endpoint);
        synchronized (incomingMessages) {
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
        return outgoingMessages.values().stream().anyMatch(raftMessages -> !raftMessages.isEmpty());
    }

    private void startSendingMessages() {
        messageSendingThread = new Thread(() -> {
            while (true) {
                while (!hasReadyMessages()) {
                    try {
                        synchronized (outgoingMessages) {
                            outgoingMessages.wait();
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }
                }

                outgoingMessages.forEach((Node<RaftMessage> node, Queue<RaftMessage> messages) ->
                        messages.forEach((RaftMessage msg) -> {
                            SocketConnection receiver = connections.get(node);
                            boolean success = receiver.send(msg);
                            if (!success) {
                                discardConnection(receiver);
                            }
                        })
                );
            }
        });

        messageSendingThread.setDaemon(true);
        messageSendingThread.start();
    }

    private TimerTask scheduleMessageTimeout(RaftMessage message) {
        queueMessage(message, message.getReceiver());
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                queueMessage(message, message.getReceiver());
            }
        };

        timeoutTimer.scheduleAtFixedRate(timerTask, message.getTimeout().toMillis(), message.getTimeout().toMillis());
        return timerTask;
    }

    public abstract void runRaft();
}
