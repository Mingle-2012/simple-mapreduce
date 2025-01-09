package site.pdli.mapreduce;

import java.util.List;

public interface Context<K, V> {
    void emit(K key, V value);
    List<String> getOutputFiles();
}
