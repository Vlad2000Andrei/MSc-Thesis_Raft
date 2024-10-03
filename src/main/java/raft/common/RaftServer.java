package raft.common;

import raft.network.Connection;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RaftServer extends Node<RaftMessage> {

    private ServerSocket serverSocket;
    private ConcurrentHashMap<Node, SocketConnection<RaftMessage>> connections;
    private List<Node> clients;
    private List<Node> servers;
    private AtomicBoolean acceptingConnections;
    private Thread acceptingConnectionsThread;
    private Thread messageReceivingThread;
    private final Map<Node, List<RaftMessage>> outgoingMessages;
    private final Map<Node, List<RaftMessage>> incomingMessages;
    private Selector incomingMessageSelector;
    private List<SelectionKey> channelSelectionKeys;

    public RaftServer(InetSocketAddress address) throws IOException {
        super(address);
        serverSocket = new ServerSocket(address.getPort());
        acceptingConnections = new AtomicBoolean();
        acceptingConnections.set(false);

        outgoingMessages = new HashMap<>();
        incomingMessages = new HashMap<>();

        channelSelectionKeys = new ArrayList<>();
    }

    public void start() {
        acceptConnections();
        startAcceptingMessages();
    }

    private SocketConnection<RaftMessage> acceptConnection() throws IOException {
        Socket socket = serverSocket.accept();
        SocketConnection<RaftMessage> connection = new SocketConnection<RaftMessage>(
                Arrays.toString(socket.getInetAddress().getAddress()),
                socket.getPort(),
                socket
        );
        return connection;
    }

    public void acceptConnections() {
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

                } catch (IOException e) {
                    System.out.println("[ERR] Failed to accept connection: " + e.getMessage());

                    if (!serverSocket.isBound()) {
                        acceptingConnections.set(false);
                        break;
                    }

                    if (serverSocket.isClosed()) {
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
    }

    public void queueMessage (RaftMessage message, List<Node<RaftMessage>> nodes) {
        for (Node<RaftMessage> node : nodes) {
            queueMessage(message, node);
        }
    }

    public void acceptOneMessage(SocketConnection<RaftMessage> connection) {
        RaftMessage message = connection.receive();
        Node<RaftMessage> sender = message.getSender();
        List<RaftMessage> buffer = incomingMessages.getOrDefault(sender, new ArrayList<>());
        buffer.add(message);
        incomingMessages.put(sender, buffer);
    }

    public void startAcceptingMessages() {
        messageReceivingThread = new Thread(() -> {
            try {
                incomingMessageSelector.select();
                incomingMessageSelector.selectedKeys()
                        .stream()
                        .map((SelectionKey k) -> (SocketConnection<RaftMessage>) k.attachment())
                        .forEach(this::acceptOneMessage);
            }
            catch (IOException e) {
                System.out.println("[ERR Could not select incoming messages: " + e.getMessage());
            }
        });
    }


}
