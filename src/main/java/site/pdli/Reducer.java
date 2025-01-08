package site.pdli;

public interface Reducer<K, V, CK, CV> {
    void reduce(K key, Iterable<V> values, Context<CK, CV> context);
}
