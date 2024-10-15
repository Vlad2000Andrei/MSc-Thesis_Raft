package raft.common;

import org.jetbrains.annotations.NotNull;
import raft.classic.EchoRaftServer;
import raft.messaging.common.ControlMessage;
import raft.messaging.common.ControlMessageType;
import raft.messaging.common.MessageStatus;
import raft.messaging.common.RaftMessage;
import raft.network.Configuration;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public abstract class RaftServer extends Node<RaftMessage> {

    private ServerSocketChannel serverSocketChannel;
    private ConcurrentHashMap<Node, SocketConnection> connections;
    protected List<Node<RaftMessage>> clients;
    protected List<Node<RaftMessage>> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private Thread messageSendingThread;
    private final ConcurrentHashMap<Node<RaftMessage>, Queue<RaftMessage>> outgoingMessages;
    protected final Timer timeoutTimer;
    protected Configuration clusterConfig;
    private Map<Long, TimerTask> timedMessages;
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
        timeoutTimer = new Timer("Timeout Timer");
        timedMessages = new ConcurrentHashMap<>();
    }

    public void start(Configuration config) {
        this.clusterConfig = config;
        Thread.currentThread().setName("Server " + id);

        // Start the different worker threads
        acceptConnections();
        startAcceptingMessages();
        startSendingMessages();
        connectToServers();

        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        runRaft();
    }

    private void connectToServers() {
        final int ATTEMPTS = 30;
        for (int i = 0; i < clusterConfig.servers().size(); i++) {
            if (i == id) continue;
            for (int j = 0; j < ATTEMPTS; j++) {
                try {
                    Thread.sleep(500);

                    Node<RaftMessage> peer = clusterConfig.servers().get(i);
                    SocketConnection connection = connectTo(peer);
                    connection.endpoint = peer;
                    System.out.printf("[i] Server %d connected to peer %s.\n", id, peer.getInetSocketAddress());
                    servers.add(peer);
                    registerConnection(connection);
                    break;
                } catch (Exception e) {
                    System.out.printf("[i] Server %d failed to connect to peer %s. Retrying...", id, clusterConfig.servers().get(i).getInetSocketAddress());
                }
            }
        }
    }

    private void discardConnection (SocketConnection connection) {
        try {
            if (connections.contains(connection.endpoint)) return;

            System.out.printf("[i] Discarding connection to %s : %s. Remaining connections: %s.\n",
                    connection.endpoint.getInetSocketAddress().getAddress().getHostAddress(),
                    connection.endpoint.getInetSocketAddress().getPort(),
                    connections.size() - 1);

            clients.remove(connection.endpoint);
            servers.remove(connection.endpoint);
            outgoingMessages.remove(connection.endpoint);
            connections.remove(connection.endpoint);
            connection.close();

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
                    System.out.printf("[i] (%s) Client (%s) connected on local port %s.\n", Thread.currentThread().getName(), remoteAddress, connection.getNonBlockingChannel().socket().getLocalPort());

                    // Store connection for reuse
                    registerConnection(connection);

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
        acceptingConnectionsThread.setName("Accepting Connections Thread");
        acceptingConnectionsThread.setDaemon(true);
        acceptingConnectionsThread.start();
    }

    private void registerConnection(SocketConnection connection) throws IOException {
        boolean isServer = clusterConfig.servers().stream().anyMatch(node -> node.equals(connection.endpoint));

        synchronized (connections) {
            if (isServer) servers.add(connection.endpoint);
            else clients.add(connection.endpoint);
            connections.put(connection.endpoint, connection);
        }
        SelectionKey selectionKey = connection.getNonBlockingChannel().register(incomingMessageSelector, SelectionKey.OP_READ);
        selectionKey.attach(connection);
        channelSelectionKeys.add(selectionKey);
        incomingMessageSelector.wakeup();
    }

    public Collection<SocketConnection> getConnections() {
        return connections.values();
    }

    public void queueMessage (RaftMessage message, @NotNull Node<RaftMessage> node) {
        message.setReceiver(node);
        synchronized (outgoingMessages) {
            Queue<RaftMessage> messageQueue = outgoingMessages.getOrDefault(node, new ConcurrentLinkedQueue<>());
            messageQueue.add(message);
            outgoingMessages.put(node, messageQueue);
            outgoingMessages.notifyAll();

            if (message.getTimeout() != null) {
                scheduleMessageTimeout(message);
            }
        }

//        System.out.println(Thread.currentThread().getName() + "\tQueued message " + message + " for " + node.getInetSocketAddress());
    }

    public void queueMessage (RaftMessage message, List<Node<RaftMessage>> nodes) {
        for (Node<RaftMessage> node : nodes) {
            message.setReceiver(node);
            queueMessage(new RaftMessage(message), node);
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

        synchronized (timedMessages) {
            TimerTask scheduledResend = timedMessages.remove(message.ackNr);    // so that it stops re-sending
            if (scheduledResend != null) {
                scheduledResend.cancel();
            }
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

        messageReceivingThread.setName("Message Receiver Thread");
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
                    System.out.printf("[!] Server %d getNextMessage() interrupted. Continuing...", id);
                    continue;
                }
            }
        }
//        System.out.printf("[Receiver] Server %d received message %s.\n", id, incomingMessages.peek());
        return incomingMessages.remove();
    }

    private boolean hasReadyMessages() {
        return outgoingMessages.values().stream().anyMatch(raftMessages -> !raftMessages.isEmpty());
    }

    private void startSendingMessages() {
        messageSendingThread = new Thread(() -> {
            while (true) {
                // wait for messages to be ready in the outgoing message queue
                while (!hasReadyMessages()) {
                    try {
                        synchronized (outgoingMessages) {
                            outgoingMessages.wait();
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }
                }

                // dequeue and send all available messages
                outgoingMessages.forEach((Node<RaftMessage> node, Queue<RaftMessage> messages) -> {
                        while (!messages.isEmpty()) {
                            RaftMessage msg = messages.remove();
                            SocketConnection receiver = connections.get(node);
                            System.out.printf("[Sender] Server %d sent: %s to %s\n", id, msg, node.getInetSocketAddress());
                            if (receiver == null) {
                                return;
                            }
                            boolean success = receiver.send(msg);
                            if (!success) {
                                discardConnection(receiver);
                            }
                        }
                    }
                );
            }
        });

        messageSendingThread.setName("Message Sender Thread");
        messageSendingThread.setDaemon(true);
        messageSendingThread.start();
    }

    private TimerTask scheduleMessageTimeout(RaftMessage message) {

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                queueMessage(message, message.getReceiver());
            }
        };

        timeoutTimer.scheduleAtFixedRate(timerTask, message.getTimeout().toMillis(), message.getTimeout().toMillis());
        timedMessages.put(message.sequenceNr, timerTask);
        message.setTimeout(null);
        return timerTask;
    }

    public abstract void runRaft();

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public String toString() {
        return String.format("Server %d at %s", id, getInetSocketAddress());
    }
}
