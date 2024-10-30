package raft.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class BenchmarkController implements Runnable {
    private final Map<Integer, ObjectOutputStream> outputStreams;
    private ServerSocket serverSocket;
    private final ObjectMapper mapper;
    private Duration timeLimit = null;
    private TimerTask stopTimerTask = null;
    private Timer stopTimer;
    private Integer numWorkers = null;
    private BenchmarkControlMessageType serverType = null;

    public BenchmarkController(String bindAddress, Integer bindPort, Integer numWorkers, Integer timeLimitMin, String serverType) {
        stopTimer = new Timer();
        outputStreams = new ConcurrentHashMap<>();
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        try {
            InetSocketAddress socketAddress = new InetSocketAddress(bindAddress, bindPort);
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            System.out.printf("Controller started on %s.\n", socketAddress);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }

        if (numWorkers != null) {
            timeLimit = Duration.ofMinutes(timeLimitMin);
            this.numWorkers = numWorkers;

            if (serverType.equals("c") || serverType.equals("classic")) {
                this.serverType = BenchmarkControlMessageType.START_CLASSIC;
            } else if (serverType.equals("m") || serverType.equals("modified")) {
                this.serverType = BenchmarkControlMessageType.START_MODIFIED;
            } else {
                System.out.println("Unknown server type " + serverType);
            }
        }
    }

    public BenchmarkController(String bindAddress, Integer bindPort) {
        this(bindAddress, bindPort, null, null, null);
    }

    private void handleConnection(ObjectInputStream fromWorker, ObjectOutputStream toWorker) {
        Runnable handler = () -> {
            Integer serverId = null;
            while(true) {
                try {
                    BenchmarkControlMessage msg = (BenchmarkControlMessage) fromWorker.readObject();
                    switch (msg.type()) {
                        case LOG_OK -> {
                            writeLogEntries(msg);
                            System.out.printf("Server %d: %d log entries.\n", msg.serverId(), msg.logEntries().size());
                        }
                        case STOP_OK -> {
                            outputStreams.remove(msg.serverId());
                            System.out.printf("Server %d stopped.\n", msg.serverId());
                            return;
                        }
                        case HELLO -> {
                            outputStreams.put(msg.serverId(), toWorker);
                            serverId = msg.serverId();
                        }
                    }
                }
                catch (SocketException se) {
                    outputStreams.remove(serverId);
                    System.out.printf("Lost connection to server %d, %d remaining.\n", serverId, outputStreams.size());

                    // If we're in hands-off mode
                    if (numWorkers != null) {
                        if (outputStreams.size() == 0)
                            System.exit(0);
                    }
                    return;
                }
                catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {}
                    System.exit(1);
                }
            }
        };
        Thread handlerThread = new Thread(handler);
        handlerThread.setDaemon(true);
        handlerThread.setName("Handler");
        handlerThread.start();
    }

    private void acceptConnections() {
        while(true) {
            try {
                System.out.println("Controller waiting for incoming connections...");
                Socket sock = serverSocket.accept();
                ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                handleConnection(ois, oos);
                System.out.printf("Accepted connection from benchmark worker at %s:%s.\n", sock.getInetAddress().getHostAddress(), sock.getPort());
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private void manageCrashes() {
        CrashController crashController = new CrashController(0.00005, 0);
        BenchmarkControlMessage crashMessage = new BenchmarkControlMessage(BenchmarkControlMessageType.CRASH, null, null);
        while(true) {
            try {
                for (int serverId : outputStreams.keySet()) {
                    if (crashController.checkCrash(serverId)) {
                        try {
                            System.out.println("Trying to crash server " + serverId);
                            outputStreams.get(serverId).writeObject(crashMessage);
                        } catch (IOException ignored) {}
                    }
                }
                Thread.sleep(1);
            }
            catch (InterruptedException ignored) {}
        }
    }

    private void writeLogEntries (BenchmarkControlMessage logOkMessage) {
        String filePath = String.format("./%d_server_log.raftlog", logOkMessage.serverId());
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            mapper.writeValue(fos, logOkMessage.logEntries());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void broadcastMessage(BenchmarkControlMessage msg) {
        try {
            for (ObjectOutputStream stream : outputStreams.values()) {
                stream.writeObject(msg);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void stopWorkers() {
        broadcastMessage(new BenchmarkControlMessage(BenchmarkControlMessageType.STOP, null, null));
    }

    private void requestLogs() {
        broadcastMessage(new BenchmarkControlMessage(BenchmarkControlMessageType.LOG, null, null));
    }

    private void startWorkers(BenchmarkControlMessageType workerType) {
        broadcastMessage(new BenchmarkControlMessage(workerType, null, null));
    }

    private void clearTimer() {
        timeLimit = null;
        stopTimerTask.cancel();
        stopTimer.cancel();
        stopTimerTask = null;
    }

    private void startScheduledRun(BenchmarkControlMessageType workerType) {
        startWorkers(workerType);
        if (timeLimit != null) {
            stopTimerTask = new TimerTask() {
                @Override
                public void run() {
                    requestLogs();
                    stopWorkers();
                }
            };
            stopTimer.schedule(stopTimerTask, timeLimit.toMillis());
        }
    }

    @Override
    public void run() {
        Thread acceptingConnectionsThread = new Thread(this::acceptConnections);
        acceptingConnectionsThread.setDaemon(true);
        acceptingConnectionsThread.start();

        Thread crashManagerThread = new Thread(this::manageCrashes);
        crashManagerThread.setDaemon(true);
        crashManagerThread.start();

        if (numWorkers != null) {
            while (outputStreams.size() < numWorkers) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ignored) {}
            }
            System.out.printf("Starting %s on all %d workers for %d minutes.\n", serverType, outputStreams.size(), timeLimit.toMinutes());
            startScheduledRun(serverType);
        }

        Scanner consoleScanner = new Scanner(System.in);
        while(true) {
            String line = consoleScanner.nextLine();
            System.out.printf("Controller parsing command: '%s'\n", line);
            try {
                switch (line.toLowerCase()) {
                    case "stop" -> {
                        System.out.printf("Stopping all %d workers.\n", outputStreams.size());
                        stopWorkers();
                    }
                    case "log", "logs" -> {
                        System.out.printf("Requesting logs of all %d workers.\n", outputStreams.size());
                        requestLogs();
                    }
                    case "start c", "start classic" -> {
                        System.out.printf("Starting classic Raft on all %d workers.\n", outputStreams.size());
                        startScheduledRun(BenchmarkControlMessageType.START_CLASSIC);
                    }
                    case "start m", "start modif", "start modified" -> {
                        System.out.printf("Starting modified Raft on all %d workers.\n", outputStreams.size());
                        startScheduledRun(BenchmarkControlMessageType.START_MODIFIED);
                    }
                    case "clear timer" -> {
                        clearTimer();
                    }
                    case "set timer" -> {
                        System.out.println("Enter timer duration in minutes: ");
                        String input = consoleScanner.nextLine().strip();
                        try {
                            int minutes = Integer.parseInt(input);
                            timeLimit = Duration.ofMinutes(minutes);
                        }
                        catch (NumberFormatException nfe) {
                            System.out.printf("Could not parse %s to number of minutes. Enter timer command again to retry.\n", input);
                        }
                    }
                    default -> System.out.printf("Unknown command: '%s'\n", line);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
