package site.pdli;

public interface Context<K, V> {
    void emit(K key, V value);
}
