package raft.common;

import raft.benchmark.CrashObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class UptimeFitnessEstimator extends FitnessEstimator implements CrashObserver {

    private Instant startInstant;
    private Duration downTime;

    private List<Instant> crashes;

    public UptimeFitnessEstimator(Instant startTime) {
        this.startInstant = startTime;
        this.crashes = new ArrayList<>();
        this.downTime = Duration.ZERO;
    }

    public UptimeFitnessEstimator() {
        this(Instant.now());
    }

    private void logCrash() {
        crashes.add(Instant.now());
    }

    private void logRecovery() {
        Instant crashInstant = crashes.get(crashes.size() - 1);
        Duration crashDuration = Duration.between(crashInstant, Instant.now());
        downTime = downTime.plus(crashDuration);
    }

    public void notifyCrash() {
        logCrash();
    }

    public void notifyRecovery() {
        logRecovery();
    }

    @Override
    public double getFitness() {
        return Duration.between(startInstant, Instant.now()).minus(downTime).toMillis();
    }
}
