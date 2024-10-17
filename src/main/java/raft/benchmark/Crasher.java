package raft.benchmark;

import raft.classic.ClassicRaftServer;
import raft.common.Colors;
import raft.common.RaftServer;

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
        if (chance > threshold) return;


        long millis = rand.nextLong(minTime.toMillis(), maxTime.toMillis());
        Instant endPoint = Instant.now().plus(Duration.ofMillis(millis));

        if (server.heartbeatTimer != null) {
            server.heartbeatTimer.cancel();
            server.heartbeatTimer = null;
        }

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

        server.scheduleHeartbeatMessages();
    }
}
