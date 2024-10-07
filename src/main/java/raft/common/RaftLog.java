package raft.common;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.BaseStream;

public class RaftLog implements Iterable<LogEntry>, Comparable<RaftLog> {

    private List<LogEntry> entries;
    private int committedIndex;
    private int lastApplied;

    public RaftLog() {
        entries = new ArrayList<>();
        committedIndex = -1;
        lastApplied = -1;
    }

    public LogEntry get(int index) {
        return entries.get(index);
    }

    public boolean add(LogEntry entry) {
        return entries.add(entry);
    }

    public boolean addAll(Collection<LogEntry> entryList) {
        return entries.addAll(entryList);
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
        return committedIndex;
    }

    public void setCommittedIndex(Integer committedIndex) {
        this.committedIndex = committedIndex;
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
}
