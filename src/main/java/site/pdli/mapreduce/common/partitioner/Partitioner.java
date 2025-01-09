package site.pdli.mapreduce.common.partitioner;

public interface Partitioner {
    int getPartition(Object key, int numPartitions);
}
