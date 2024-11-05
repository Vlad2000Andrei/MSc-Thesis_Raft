package raft.benchmark;

import java.util.Random;

public class CrashController {
    private double crashChance;
    private double serverRankBias;
    private Random random;

    public CrashController (double crashChancePerMilli, double serverRankBias) {
        this.crashChance = crashChancePerMilli * 1_000_000_000;
        this.serverRankBias = serverRankBias * 1_000_000_000;
        random = new Random();
    }

    public boolean checkCrash (int serverRank, int outOfTotalServers) {
        double chance = random.nextInt(1_000_000_000);
        double crashThreshold = crashChance + ((outOfTotalServers - serverRank) * serverRankBias);

        return chance < crashThreshold;
    }
}
