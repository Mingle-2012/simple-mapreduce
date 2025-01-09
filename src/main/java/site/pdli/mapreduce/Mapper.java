package site.pdli.mapreduce;

public interface Mapper<K, V, CK, CV> {
    void map(K key, V value, Context<CK, CV> context);
}
