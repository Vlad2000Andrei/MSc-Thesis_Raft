package raft.benchmark;

public interface CrashObserver {

    void notifyCrash();

    void notifyRecovery();

}
