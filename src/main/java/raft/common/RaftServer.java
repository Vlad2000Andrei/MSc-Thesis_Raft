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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public abstract class RaftServer extends Node<RaftMessage> {
    protected final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(200);
    protected final Duration ELECTION_TIMEOUT_MIN = Duration.ofMillis(3000);
    protected final Duration ELECTION_TIMEOUT_MAX = Duration.ofMillis(6000);
    protected final Duration MSG_RETRY_INTERVAL = Duration.ofMillis(200);
    protected Duration electionTimeout = Duration.ofSeconds(5);
    protected Instant electionTimeoutStartInstant;


    private ServerSocketChannel serverSocketChannel;
    protected ConcurrentHashMap<Node, SocketConnection> connections;
    protected HashSet<Node<RaftMessage>> clients;
    protected HashSet<Node<RaftMessage>> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private Thread messageSendingThread;
    protected final Timer timeoutTimer;
    protected Configuration clusterConfig;
    private final Map<Long, TimerTask> timedMessages;
    protected final LinkedBlockingQueue<RaftMessage> outgoingMessages;
    protected final LinkedBlockingQueue<RaftMessage> incomingMessages;
    private Selector incomingMessageSelector;
    private List<SelectionKey> channelSelectionKeys;


    public RaftServer(InetSocketAddress address) throws IOException {
        super(address);
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(address);
        acceptingConnections = new AtomicBoolean();
        acceptingConnections.set(false);

        clients = new HashSet<>();
        servers = new HashSet<>();

        connections = new ConcurrentHashMap<>();
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();
        incomingMessageSelector = Selector.open();

        channelSelectionKeys = new ArrayList<>();
        timeoutTimer = new Timer("Timeout Timer");
        timedMessages = new ConcurrentHashMap<>();
    }

    public void start(Configuration config) {
        this.clusterConfig = config;
        System.out.printf("Server %d starting...\n", id);
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
            if (clusterConfig.servers().get(i).id <= this.id) continue;
            for (int j = 0; j < ATTEMPTS; j++) {
                try {
                    Thread.sleep(500);

                    Node<RaftMessage> peer = clusterConfig.servers().get(i);
                    SocketConnection connection = connectTo(peer);
                    registerConnection(connection);
                    servers.add(connection.endpoint);
                    System.out.printf("[Infra] Server %d connected to %s. It now knows: \n\t- Servers: %s \n\t- Connections: %s\n", id, connection.endpoint, servers, connections.values());

                    queueMessage(new RaftMessage(new ControlMessage(ControlMessageType.HELLO_SERVER, 0, true, -1)), connection.endpoint);
                    break;
                } catch (Exception e) {
                    System.out.printf("[i] Server %d failed to connect to peer %s. Retrying...\n", id, clusterConfig.servers().get(i).getInetSocketAddress());
                }
            }
        }
    }

    public int quorumSize() {
        if (clusterConfig.servers().size() == 2) return 2;
        else return (clusterConfig.servers().size() + 1) / 2;
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
                socketChannel.socket().getInetAddress().getHostAddress(),
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
        acceptingConnectionsThread.setName("Accepting Connections Thread " + id);
        acceptingConnectionsThread.setDaemon(true);
        acceptingConnectionsThread.start();
    }

    private void registerConnection(SocketConnection connection) throws IOException {
        connections.put(connection.endpoint, connection);
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
        outgoingMessages.add(message);
        scheduleMessageTimeout(message);
    }

    public void queueMessage (RaftMessage message, List<Node<RaftMessage>> nodes) {
        for (Node<RaftMessage> node : nodes) {
            queueMessage(new RaftMessage(message), node);
        }
    }

    public void queueServerBroadcast (RaftMessage message) {
        System.out.printf("[Broadcast] Server %d broadcasting %s to %d servers: %s. Queue size: %d\n", id, message, servers.size(), servers.stream().toList(), outgoingMessages.size());
        queueMessage(message, servers.stream().toList());
    }

    private void acceptOneMessage(SocketConnection connection) {
        RaftMessage message = connection.receive();
        if (message == null) {
            discardConnection(connection);
            return;
        }
        message.setSender(connection.endpoint);
        incomingMessages.add(message);


        TimerTask scheduledResend = timedMessages.remove(message.ackNr);    // so that it stops re-sending
        if (scheduledResend != null) {
            scheduledResend.cancel();
        }
    }

    private void startAcceptingMessages() {
        messageReceivingThread = new Thread(() -> {
            while (true) {
                try {
                    incomingMessageSelector.select();
                    incomingMessageSelector.selectedKeys()
                            .stream()
                            .forEach(key -> {
                                SocketConnection conn = (SocketConnection) key.attachment();
                                acceptOneMessage(conn);
                                incomingMessageSelector.selectedKeys().remove(key);
                            });
                } catch (IOException e) {
                    System.out.println("[ERR Could not select incoming messages: " + e.getMessage());
                }
            }
        });

        messageReceivingThread.setName("Message Receiver Thread " + id);
        messageReceivingThread.setDaemon(true);
        messageReceivingThread.start();
    }

    public RaftMessage getNextMessage() {
        Instant nextElectionTimeout = electionTimeoutStartInstant.plus(electionTimeout);
        Duration timeUntilElectionTimeout = Duration.between(Instant.now(), nextElectionTimeout);
        try {
            return incomingMessages.poll(timeUntilElectionTimeout.toMillis() / 10, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            return null;
        }
    }

    private void startSendingMessages() {
        messageSendingThread = new Thread(() -> {
            while (true) {
                try {
                    RaftMessage msg = outgoingMessages.take();
                    SocketConnection conn = connections.get(msg.getReceiver());
                    if (conn == null) {
                        System.out.printf("[!] %s could not send message %s: unknown connection.\n", this, msg);
                        continue;
                    }
//                    System.out.printf("[Sender] Server %d sending %s to %s.\n", id, msg, msg.getReceiver());
                    boolean success = conn.send(msg);
                    if (!success) {
                        discardConnection(conn);
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        messageSendingThread.setName("Message Sender Thread " + id);
        messageSendingThread.setDaemon(true);
        messageSendingThread.start();
    }

    private TimerTask scheduleMessageTimeout(RaftMessage message) {
        if (message.getTimeout() == null) return null;
        long timeoutMilis = message.getTimeout().toMillis();
        message.setTimeout(null);

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                System.out.printf("[Timeouts] Server %d Re-Queueing message %s. Still in queue: %s\n", id, message, timedMessages.containsKey(message.sequenceNr));
                queueMessage(message, message.getReceiver());
            }
        };

        timeoutTimer.scheduleAtFixedRate(timerTask, timeoutMilis, timeoutMilis);
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
        return String.format("Server %d", id);
    }
}
