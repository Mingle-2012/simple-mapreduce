package site.pdli.common.partitioner;

public class HashcodePartitioner implements Partitioner {
    @Override
    public int getPartition(Object key, int numPartitions) {
        return Math.abs(key.hashCode() % numPartitions);
    }
}
