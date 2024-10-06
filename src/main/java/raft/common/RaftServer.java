package raft.common;

import org.jetbrains.annotations.NotNull;
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
    private int received = 0;
    private int sent = 0;

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
                    connection.endpoint = new Node<RaftMessage>(remoteAddress);
                    System.out.printf("[i] (%s) Client (%s) connected on local port %s.\n",
                            Thread.currentThread().getName(),
                            remoteAddress.toString(),
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
        received++;
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
                                SocketConnection receiver = connections.get(node);
                                boolean success = receiver.send(msg);
                                sent++;
                                if (!success) {
                                    discardConnection(receiver);
                                }
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
