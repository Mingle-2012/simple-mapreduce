package site.pdli.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.Config;
import site.pdli.messaging.Worker.TaskType;
import site.pdli.messaging.Worker.WorkerStatus;
import site.pdli.messaging.WorkerClient;
import site.pdli.task.TaskInfo;
import site.pdli.utils.FileUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("FieldMayBeFinal")
public class Master extends WorkerBase {
    private static final Logger log = LoggerFactory.getLogger(Master.class);
    private Map<String, WorkerContext> mappers = new HashMap<>();
    private Map<String, WorkerContext> reducers = new HashMap<>();
    private Map<String, TaskInfo> tasks = new HashMap<>();
    private Map<Integer, String> partsToReducerId = new HashMap<>();

    /**
     * parts to completed count(maximum is number of mappers)
     */
    private Map<String, Integer> partsToCompletedCnt = new HashMap<>();


    private AtomicInteger reducersFinished = new AtomicInteger(0);

    public Master(String id, int port) {
        super(id, port);
    }

    public void addMapper(String id, String host, int port) {
        mappers.put(id, new WorkerContext(host, port));
    }

    public void addReducer(String id, String host, int port) {
        reducers.put(id, new WorkerContext(host, port));
        var nowParts = partsToReducerId.size();
        partsToReducerId.put(nowParts, id);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void splitInput() throws IOException {
        var numMappers = mappers.size();
        var inputFile = Config.getInstance()
            .getInputFile();
        var lines = FileUtil.readLocal(inputFile.getPath())
            .lines()
            .toList();

        var linesPerMapper = lines.size() / numMappers;

        for (int i = 0; i < numMappers; i++) {
            var start = i * linesPerMapper;
            var end = (i + 1) * linesPerMapper;
            if (i == numMappers - 1) {
                end = lines.size();
            }
            var linesForMapper = lines.subList(start, end);
            var tmpDir = Config.getInstance()
                .getTmpDir()
                .getPath();
            var inputFileForMapper = tmpDir + "/" + host + "/" + port + "/input-" + i;
            FileUtil.writeLocal(inputFileForMapper, String.join("\n", linesForMapper)
                .getBytes());
        }
    }

    public void assignMapTask() {
        var tmpDir = Config.getInstance()
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
        while (reducersFinished.get() < reducers.size()) ;

        log.info("All reducers finished.");
    }

    @Override
    public void start() {
        super.start();
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
            reducersFinished.set(reducersFinished.get() + 1);
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
