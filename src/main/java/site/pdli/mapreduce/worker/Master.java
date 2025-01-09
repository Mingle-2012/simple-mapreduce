package site.pdli.mapreduce.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.mapreduce.Config;
import site.pdli.mapreduce.messaging.Worker.TaskType;
import site.pdli.mapreduce.messaging.Worker.WorkerStatus;
import site.pdli.mapreduce.messaging.WorkerClient;
import site.pdli.mapreduce.task.TaskInfo;
import site.pdli.mapreduce.utils.FileUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("FieldMayBeFinal")
public class Master extends WorkerBase {
    private static final Logger log = LoggerFactory.getLogger(Master.class);
    private List<WorkerContext> idleWorkers = new ArrayList<>();

    public Map<String, WorkerContext> getMappers() {
        return mappers;
    }

    public Map<String, WorkerContext> getReducers() {
        return reducers;
    }

    private Map<String, WorkerContext> mappers = new HashMap<>();
    private Map<String, WorkerContext> reducers = new HashMap<>();
    private Map<String, TaskInfo> tasks = new HashMap<>();
    private Map<Integer, String> partsToReducerId = new HashMap<>();

    /**
     * parts to completed count(maximum is number of mappers)
     */
    private Map<String, Integer> partsToCompletedCnt = new HashMap<>();


    private CountDownLatch reducersFinishedLatch;

    private Config config = Config.getInstance();

    public Master(String id, int port) {
        super(id, port);
        log.info("Master {} started at {}:{}", id, host, port);
    }

    private void addMapper(String id, WorkerContext ctx) {
        mappers.put(id, ctx);
    }

    private void addReducer(String id, WorkerContext ctx) {
        reducers.put(id, ctx);
        var nowParts = partsToReducerId.size();
        partsToReducerId.put(nowParts, id);
    }

    public void addWorker(String host, int port) {
        idleWorkers.add(new WorkerContext(host, port));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void splitInput() throws IOException {
        if (config.getSplitMethod() == Config.SplitMethod.BY_CHUNK_SIZE) {
            splitByChunkSize();
        } else {
            splitByNumMappers();
        }

        var numMappers = config.getNumMappers();
        var numReducers = config.getNumReducers();

        for (int i = 0; i < numMappers; i++) {
            var mapperId = "mapper-" + i;
            if (idleWorkers.isEmpty()) {
                throw new RuntimeException("Not enough workers for mappers");
            }
            var first = idleWorkers.get(0);
            addMapper(mapperId, first);
            idleWorkers.remove(0);
        }

        for (int i = 0; i < numReducers; i++) {
            var reducerId = "reducer-" + i;
            if (idleWorkers.isEmpty()) {
                throw new RuntimeException("Not enough workers for reducers");
            }
            var first = idleWorkers.get(0);
            addReducer(reducerId, first);
            idleWorkers.remove(0);
        }
    }

    private void splitByNumMappers() throws IOException {
        var numMappers = config.getNumMappers();
        var inputFile = config
            .getInputFile();
        var lines = FileUtil.readLocal(inputFile.getPath())
            .lines()
            .toList();

        var linesPerMapper = lines.size() / numMappers;
        var tmpDir = config
            .getTmpDir()
            .getPath();

        for (int i = 0; i < numMappers; i++) {
            var start = i * linesPerMapper;
            var end = (i + 1) * linesPerMapper;
            if (i == numMappers - 1) {
                end = lines.size();
            }
            var linesForMapper = lines.subList(start, end);
            var inputFileForMapper = tmpDir + "/" + host + "/" + port + "/input-" + i;
            FileUtil.writeLocal(inputFileForMapper, String.join("\n", linesForMapper)
                .getBytes());
        }
    }

    private void splitByChunkSize() throws IOException {
        var inputFile = config
            .getInputFile();
        var lines = FileUtil.readLocal(inputFile.getPath())
            .lines()
            .toList();
        var chunkSize = config
            .getSplitChunkSize();
        var tmpDir = config
            .getTmpDir()
            .getPath();

        int currentGroupSize = 0;
        int i = 0;
        List<String> currentGroupLines = new ArrayList<>();

        for (var line : lines) {
            int lineSize = line.getBytes().length;
            if (currentGroupSize + lineSize > chunkSize && !currentGroupLines.isEmpty()) {
                var inputFileForMapper = tmpDir + "/" + host + "/" + port + "/input-" + i;
                FileUtil.writeLocal(inputFileForMapper, String.join("\n", currentGroupLines)
                    .getBytes());

                currentGroupSize = 0;
                currentGroupLines.clear();
                i++;
            }

            currentGroupSize += lineSize;
            currentGroupLines.add(line);
        }

        if (!currentGroupLines.isEmpty()) {
            var inputFileForMapper = tmpDir + "/" + host + "/" + port + "/input-" + i;
            FileUtil.writeLocal(inputFileForMapper, String.join("\n", currentGroupLines)
                .getBytes());
        }

        config.setNumMappers(i + 1);
    }

    public void assignMapTask() {
        var tmpDir = config
            .getTmpDir()
            .getPath();

        int i = 0;
        for (var entry : mappers.entrySet()) {
            var inputFileForMapper = makeFile(tmpDir + "/" + host + "/" + port + "/input-" + i++);
            var taskId = "task-mapper-" + entry.getKey();
            var taskInfo = new TaskInfo(taskId, TaskType.MAP, List.of(inputFileForMapper));
            tasks.put(taskId, taskInfo);
            try (var client = new WorkerClient(entry.getValue()
                .getHost(), entry.getValue()
                .getPort())) {
                client.sendTask(entry.getKey(), taskInfo);
            }
        }
    }

    private String makeFile(String fileName) {
        return FileUtil.makeFile(host, port, fileName);
    }

    public void block() {
        try {
            reducersFinishedLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("[IMPORTANT] - All reducers finished.");
        log.info("Terminating workers");

        for (var entry : mappers.entrySet()) {
            try (var client = new WorkerClient(entry.getValue()
                .getHost(), entry.getValue()
                .getPort())) {
                client.sendTask(entry.getKey(),
                    new TaskInfo("terminate-task-mapper-" + entry.getKey(), TaskType.TERMINATE, List.of()));
            }
        }

        for (var entry : reducers.entrySet()) {
            try (var client = new WorkerClient(entry.getValue()
                .getHost(), entry.getValue()
                .getPort())) {
                client.sendTask(entry.getKey(),
                    new TaskInfo("terminate-task-reducer-" + entry.getKey(), TaskType.TERMINATE, List.of()));
            }
        }
    }

    @Override
    public void start() {
        super.start();
        var numReducers = config.getNumReducers();
        reducersFinishedLatch = new CountDownLatch(numReducers);
    }

    @Override
    protected void onWorkerFileWriteComplete(String workerId, List<String> outputFiles) {
        log.info("Worker {} completed with output files {}", workerId, outputFiles);
        if (mappers.containsKey(workerId)) {
            for (String outputFile : outputFiles) {
                String part = outputFile.split("-")[1];
                var taskId = "part-" + part + "-" + outputFile + "-reducer-read";
                var taskInfo = new TaskInfo(taskId, TaskType.REDUCE_READ, List.of(outputFile));
                var reducerId = partsToReducerId.get(Integer.parseInt(part));
                var client = new WorkerClient(reducers.get(reducerId)
                    .getHost(), reducers.get(reducerId)
                    .getPort());

                client.sendTask(reducerId, taskInfo);

                partsToCompletedCnt.putIfAbsent(part, 0);
                partsToCompletedCnt.put(part, partsToCompletedCnt.get(part) + 1);

                if (partsToCompletedCnt.get(part) == mappers.size()) {
                    var taskId2 = "task-reducer-" + part;
                    var taskInfo2 = new TaskInfo(taskId2, TaskType.REDUCE, List.of());
                    client.sendTask(reducerId, taskInfo2);
                }

                client.close();
            }
        } else if (reducers.containsKey(workerId) && !outputFiles.isEmpty()) {
            outputFiles.forEach(f -> {
                var data = FileUtil.readRemote(f);
                try {
                    FileUtil.writeLocal(FileUtil.getFileName(f), data.getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            reducersFinishedLatch.countDown();
        }
    }

    @Override
    protected void onHeartBeatArrive(String workerId, WorkerStatus status) {
        log.trace("Heartbeat received from worker {} with status {}", workerId, status);
    }

    @Override
    protected boolean onTaskArrive(String workerId, TaskInfo taskInfo) {
        throw new UnsupportedOperationException("Master does not receive tasks");
    }
}
