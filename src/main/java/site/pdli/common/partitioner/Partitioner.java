package site.pdli.common.partitioner;

public interface Partitioner {
    int getPartition(Object key, int numPartitions);
}
