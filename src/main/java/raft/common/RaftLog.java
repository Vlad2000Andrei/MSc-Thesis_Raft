package raft.common;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.BaseStream;

public class RaftLog implements Iterable<LogEntry>, Comparable<RaftLog> {

    private List<LogEntry> entries;
    public int committedIndex;
    public int lastApplied;

    public RaftLog() {
        entries = List.of(new LogEntry(0));
        committedIndex = 0;
        lastApplied = 0;
    }

    public LogEntry get(int index) {
        return entries.get(index);
    }

    public boolean add(LogEntry entry) {
        lastApplied++;
        return entries.add(entry);
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
