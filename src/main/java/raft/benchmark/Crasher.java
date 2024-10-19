package raft.benchmark;

import raft.classic.ClassicRaftServer;
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

    public Crasher (double probability, Duration minTime, Duration maxTime) {
        threshold = (int) (1_000_000_000 * probability);
        this.minTime = minTime;
        this.maxTime = maxTime;
        rand = new Random();
    }

    public Crasher (double probability) {
        this(
                probability,
                Duration.ofSeconds(5),
                Duration.ofSeconds(15)
        );
    }

    public Crasher setSeed(long seed) {
        rand.setSeed(seed);
        return this;
    }

    public void tryCrash(ClassicRaftServer server) {
        int chance = rand.nextInt(1_000_000_000);
//        System.out.printf("[Crasher] chance: %d \tthresh: %d\n", chance, threshold);
        if (chance > threshold) return;

        crash(server);
    }

    public void crash(ClassicRaftServer server) {
        long millis = rand.nextLong(minTime.toMillis(), maxTime.toMillis());
        Instant endPoint = Instant.now().plus(Duration.ofMillis(millis));

        server.descheduleHeartbeatMessage();

        System.out.printf(Colors.RED + "-------=========[CRASH]=========-------\n Crashing for %ds %dms\n-------=========#######=========-------\n" + Colors.RESET,
                Duration.ofMillis(millis).toSecondsPart(),
                Duration.ofMillis(millis).toMillisPart());

        while (Instant.now().isBefore(endPoint)) {
            try {
                long toSleep = Duration.between(Instant.now(), endPoint).toMillis();
                Thread.sleep(toSleep);
            }
            catch (InterruptedException e) {
                continue;
            }
        }

        System.out.println(Colors.RED + "[RECOVERY] Recovered from crash!" + Colors.RESET);

        if (server.getRole() == ServerRole.LEADER) server.scheduleHeartbeatMessages();
        server.clearElectionTimeout();
    }
}
