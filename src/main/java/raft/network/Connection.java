package raft.network;

import java.io.Serializable;
import java.util.Collection;


public interface Connection<T extends Serializable> extends AutoCloseable {

    boolean send (T value);

    T receive ();
}
