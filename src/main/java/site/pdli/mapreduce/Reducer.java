package site.pdli.mapreduce;

import java.util.List;

public interface Reducer<K, V, CK, CV> {
    void reduce(K key, List<V> values, Context<CK, CV> context);
}
