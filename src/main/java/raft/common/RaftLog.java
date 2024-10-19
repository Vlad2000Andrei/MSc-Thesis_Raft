package raft.common;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftLog implements Iterable<LogEntry>, Comparable<RaftLog> {

    private final List<LogEntry> entries;
    public AtomicInteger committedIndex;
    private AtomicInteger lastApplied;
    public AtomicInteger size;

    public RaftLog() {
        entries = new ArrayList<>(List.of(new LogEntry(0)));
        committedIndex = new AtomicInteger(0);
        lastApplied = new AtomicInteger(0);
        size = new AtomicInteger(entries.size());
    }

    public LogEntry get(int index) {
        return entries.get(index);
    }

    public void add(LogEntry entry) {
        synchronized (entries) {
            entries.add(entry);
            setSize(entries.size());
        }
    }

    public List<LogEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    @NotNull
    @Override
    public Iterator<LogEntry> iterator() {
        return entries.iterator();
    }

    public Integer getCommittedIndex() {
        return committedIndex.get();
    }

    public void setCommittedIndex(Integer committedIndex) {
        this.committedIndex.set(committedIndex);
    }

    @Override
    public int compareTo(@NotNull RaftLog that) {
        if (entries.getLast().term() != that.entries.getLast().term()) {
            return entries.getLast().term() - that.entries.getLast().term();
        }
        else {
            return entries.size() - that.entries.size();
        }
    }

    public boolean asUpToDateAs(RaftLog log) {
        return compareTo(log) >= 0;
    }

    public boolean otherAsUpToDateAsThis(int otherLastTerm, int otherLastAppliedIdx) {
        boolean result;
        if (getLast().term() != otherLastTerm) {
            result = getLast().term() <= otherLastTerm;
        }
        else {
            result = committedIndex.get() <= otherLastAppliedIdx;
        }
        return result;
    }

    public LogEntry getLast() {
        synchronized (entries) {
            return entries.get(getLastIndex());
        }
    }

    public int getLastIndex() {
        return getSize() - 1;
    }

    public boolean hasMatchingEntry (int idx, int term) {
        if (idx >= getSize()) return false;
        if (entries.get(idx).term() != term) return false;
        else return true;
    }

    public void insertEntry (int idx, LogEntry entry) {
        synchronized (entries) {
            // Remove any entries after this
            while (entries.size() > idx) {
                System.out.printf(Colors.CYAN + "Removing entry %s at %d to insert %s at %d.\n" + Colors.RESET, entries.getLast(), entries.size()-1, entry, idx);
                entries.removeLast();
            }
            setSize(entries.size());
            // Add the new one
            if (entry != null) add(entry);
        }
    }

    public int getSize() {
        return size.get();
    }

    private void setSize(int newSize) {
        if (newSize <= committedIndex.get()) {
            System.out.printf(Colors.CYAN + "[RaftLog] Illegal log shrink to %d (below commit index of %d) detected!\n", newSize, committedIndex.get());
            System.exit(1);
        }
        size.set(newSize);
    }
}
