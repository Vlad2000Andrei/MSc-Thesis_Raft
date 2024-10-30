package raft.benchmark;

import raft.servers.ClassicRaftServer;
import raft.common.Colors;
import raft.common.ServerRole;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class Crasher {

    private int threshold;
    private Duration minTime;
    private Duration maxTime;
    private Random rand;
    private Instant lastCheck;
    private Instant lastCrash;
    private Duration minTimeBetween;

    public Crasher (double probabilityPerMilli, Duration minTime, Duration maxTime, Duration minTimeBetween) {
        threshold = (int) (1_000_000_000 * probabilityPerMilli);
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.minTimeBetween = minTimeBetween;
        rand = new Random();
        lastCheck = Instant.now();
        lastCrash = Instant.now();
    }

    public Crasher (double probability) {
        this(
                probability,
                Duration.ofSeconds(5),
                Duration.ofSeconds(15),
                Duration.ofSeconds(10)
        );
    }

    public Crasher setSeed(long seed) {
        rand.setSeed(seed);
        return this;
    }

    public void tryCrash(ClassicRaftServer server) {
        tryCrash(server, false);
    }

    public void tryCrash(ClassicRaftServer server, boolean preferLeader) {
        if (Duration.between(lastCrash, Instant.now()).toMillis() < minTimeBetween.toMillis()) return;

        if (!server.controllerCrashes) {
            if (Duration.between(lastCheck, Instant.now()).toMillis() < 1) return;

            int chance = rand.nextInt(1_000_000_000);
            if (preferLeader && server.getRole() == ServerRole.LEADER) chance = chance / 2;
            if (chance > threshold) return;

            crash(server);
        }
        else if (server.crashNow) {
            crash(server);
            server.crashNow = false;
        }
    }

    public void crash(ClassicRaftServer server) {
        long millis = rand.nextLong(minTime.toMillis(), maxTime.toMillis());
        Instant endPoint = Instant.now().plus(Duration.ofMillis(millis));

        server.descheduleHeartbeatMessage();

        System.out.printf(Colors.RED + "-------=========[CRASH]=========-------\n Crashing for %ds %dms. Last crash %dms ago.\n-------=========#######=========-------\n" + Colors.RESET,
                Duration.ofMillis(millis).toSecondsPart(),
                Duration.ofMillis(millis).toMillisPart(),
                Duration.between(lastCrash, Instant.now()).toMillis());

        // Sleep for crash time to block thread.
        while (Instant.now().isBefore(endPoint)) {
            try {
                long toSleep = Duration.between(Instant.now(), endPoint).toMillis();
                Thread.sleep(toSleep);
            }
            catch (InterruptedException | IllegalArgumentException ignored) { } // if we get interrupted while sleeping or the Duration.between is negative
        }

        System.out.println(Colors.RED + "[RECOVERY] Recovered from crash!" + Colors.RESET);

        // Pretend that we crashed and our volatile in-RAM data is gone :((
        server.incomingMessages.clear();
        server.outgoingMessages.clear();

        if (server.getRole() == ServerRole.LEADER) server.scheduleHeartbeatMessages();
        server.clearElectionTimeout();
        lastCrash = Instant.now();
    }
}
