package raft.benchmark;

import java.util.Random;

public class CrashController {
    private double crashChance;
    private double serverRankBias;
    private Random random;

    public CrashController (double crashChancePerMilli, double serverRankBias) {
        this.crashChance = crashChancePerMilli * 1_000_000_000;
        this.serverRankBias = serverRankBias;
        random = new Random();
    }

    public boolean checkCrash (int serverRank) {
        double chance = random.nextInt(1_000_000_000);
        chance += serverRank * serverRankBias;

        return chance < crashChance;
    }
}