package raft.common;

import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftLog implements Iterable<LogEntry>, Comparable<RaftLog> {

    private final LinkedList<LogEntry> entries;
    public AtomicInteger committedIndex;
    private AtomicInteger lastApplied;
    public AtomicInteger size;

    public RaftLog() {
        entries = new LinkedList<>(List.of(new LogEntry(0, -1, null, null)));
        committedIndex = new AtomicInteger(0);
        lastApplied = new AtomicInteger(0);
        size = new AtomicInteger(entries.size());
    }

    public LogEntry get(int index) {
        return entries.get(index);
    }

    public void add(LogEntry entry) {
        synchronized (entries) {
            entry = new LogEntry(entry.term(), entry.leaderId(), entry.creationTime(), Instant.now()); // set storage time
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

    public void setCommittedIndex(Integer newIndex) {
//        if(newIndex > committedIndex.get()) System.out.printf(Colors.CYAN + "[RaftLog] All entries up to %d now committed. (Last Idx: %d)\n" + Colors.RESET, newIndex, getLastIndex());
        committedIndex.set(newIndex);
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
            result = getLastIndex() <= otherLastAppliedIdx;
        }
        return result;
    }

    public boolean asUpToDateAsOther(int otherLastTerm, int otherLastAppliedIdx) {
        boolean result;
        if (getLast().term() != otherLastTerm) {
            result = getLast().term() >= otherLastTerm;
        }
        else {
            result = getLastIndex() >= otherLastAppliedIdx;
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
            // If there are conflicting entries, remove them
            while (entries.size() > idx) {

//                System.out.printf(Colors.CYAN + "Removing entry %s at %d to insert %s at %d.\n" + Colors.RESET, entries.getLast(), entries.size()-1, entry, idx);
                entries.removeLast();
            }
            if (entry != null) add(entry);
            setSize(entries.size());
        }
    }

    public int getSize() {
        return size.get();
    }

    private void setSize(int newSize) {
        if (newSize <= committedIndex.get()) {
//            System.out.printf(Colors.CYAN + "[RaftLog] Log shrink to %d (below commit index of %d) detected!\n", newSize, committedIndex.get());
            // Paper says commitIndex increases monotonically, but the TLA+ spec overwrites it anyways, so we'll go with it?
//            System.exit(1);
        }
        size.set(newSize);
    }

    public boolean contains(LogEntry entry, int index) {
        if (index > getLastIndex()) return false;
        return get(index).term() == entry.term();
    }

    @Override
    public String toString() {
        return String.format("Raft Log: %s. Committed: %d LastIdx: %d Size: %d.\n", entries.toString(), getCommittedIndex(), getLastIndex(), getSize());
    }
}
