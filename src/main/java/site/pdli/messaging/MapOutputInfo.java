package site.pdli.messaging;

/**
 * MapOutputInfo: 保存Map任务输出的位置及分区信息
 */
public class MapOutputInfo {
    private final String filePath; // 输出文件的路径
    private final long offset;     // 数据的起始偏移量
    private final long length;     // 数据的长度
    private final int partition;   // 对应的分区编号

    // 构造函数
    public MapOutputInfo(String filePath, long offset, long length, int partition) {
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
        this.partition = partition;
    }

    // Getter方法
    public String getFilePath() {
        return filePath;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public int getPartition() {
        return partition;
    }

    // toString方法，用于调试和日志
    @Override
    public String toString() {
        return "MapOutputInfo{" +
            "filePath='" + filePath + '\'' +
            ", offset=" + offset +
            ", length=" + length +
            ", partition=" + partition +
            '}';
    }
}
