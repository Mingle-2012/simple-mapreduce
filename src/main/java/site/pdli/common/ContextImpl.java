package site.pdli.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.Config;
import site.pdli.Context;
import site.pdli.common.partitioner.Partitioner;
import site.pdli.utils.FileUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("fieldCanBeFinal")
public class ContextImpl<K, V> implements Context<K, V>, AutoCloseable {
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @SuppressWarnings("FieldMayBeFinal")
    private List<Tuple<K, V>> buffer = new ArrayList<>();
    @SuppressWarnings("FieldMayBeFinal")
    private List<String> outputFiles = new ArrayList<>();

    private final Lock lock = new ReentrantLock();

    private final Partitioner partitioner;
    private final int partitions = Config.getInstance().getNumReducers();
    private final String outputDir;

    private static final Logger log = LoggerFactory.getLogger(ContextImpl.class);

    public ContextImpl(Partitioner partitioner, String outputDir) {
        this.partitioner = partitioner;
        this.outputDir = outputDir;

        for (int i = 0; i < partitions; i++) {
            outputFiles.add(null);
        }

        executor.scheduleAtFixedRate(() -> {
            try {
                writeToLocal(partitioner, partitions);
            } catch (Exception e) {
                log.error("Error writing to files", e);
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void writeToLocal(Partitioner partitioner, int partitions) throws IOException {
        lock.lock();
        var bufferCopy = List.copyOf(buffer);
        buffer.clear();
        lock.unlock();

        log.info("Writing to files, Buffer size: {}", bufferCopy.size());

        List<StringBuilder> fileContents = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            fileContents.add(new StringBuilder());
        }

        for (Tuple<K, V> tuple : bufferCopy) {
            var partition = partitioner.getPartition(tuple.key(), partitions);
            if (outputFiles.get(partition) == null) {
                outputFiles.set(partition, "part-" + partition);
            }
            log.trace("Partitioned tuple ({}, {}) to partition {}", tuple.key(), tuple.value(), partition);
            try {
                fileContents.get(partition).append(tuple.toFileString()).append("\n");
            } catch (Exception e) {
                log.error("Error writing to file", e);
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < partitions; i++) {
            var content = fileContents.get(i).toString();
            if (!content.isEmpty()) {
                var fileName = outputDir + "/" + outputFiles.get(i);
                log.info("Writing to file {}", fileName);
                FileUtil.appendLocal(fileName, content.getBytes());
            }
        }
    }

    @Override
    public void emit(K key, V value) {
        lock.lock();
        buffer.add(new Tuple<>(key, value));
        log.trace("Emitted tuple ({}, {})", key, value);
        lock.unlock();
    }

    @Override
    public List<String> getOutputFiles() {
        return outputFiles;
    }

    @Override
    public void close() {
        try {
            writeToLocal(partitioner, partitions);
        } catch (IOException e) {
            log.error("Error writing to files", e);
            throw new RuntimeException(e);
        }
        executor.shutdown();
    }
}
