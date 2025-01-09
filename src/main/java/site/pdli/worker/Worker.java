package site.pdli.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.Config;
import site.pdli.messaging.Worker.TaskType;
import site.pdli.messaging.Worker.WorkerStatus;
import site.pdli.messaging.WorkerClient;
import site.pdli.task.Task;
import site.pdli.task.TaskInfo;
import site.pdli.utils.FileUtil;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;

public class Worker extends WorkerBase {
    protected String masterHost;
    protected int masterPort;
    protected WorkerStatus status = WorkerStatus.IDLE;

    protected Logger log = LoggerFactory.getLogger(Worker.class);

    protected WorkerClient workerClient;
    protected ScheduledExecutorService workerExecutor = Executors.newScheduledThreadPool(1);

    protected WorkerContext ctx;

    private final Queue<TaskInfo> taskQueue = new ConcurrentLinkedQueue<>();

    private final CountDownLatch terminateLatch = new CountDownLatch(1);

    public Worker(String id, int port, String masterHost, int masterPort) {
        super(id, port);
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.workerClient = new WorkerClient(masterHost, masterPort);

        this.ctx = new WorkerContext(host, port);

        log.info("Worker {} started at {}:{}", id, host, port);
    }

    public void block() {
        try {
            terminateLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("[IMPORTANT] - Worker {} finished", id);
    }

    @Override
    public void start() {
        super.start();
        startHeartBeating();
    }

    @Override
    public void close() {
        super.close();
        workerExecutor.shutdown();
        workerClient.close();

        var tmpDir = Config.getInstance()
            .getTmpDir();
        try {
            if (tmpDir.exists()) {
                FileUtil.del(tmpDir.getPath());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onHeartBeatArrive(String workerId, WorkerStatus status) {
        throw new UnsupportedOperationException("Worker should not receive heartbeat");
    }

    @Override
    protected void onWorkerFileWriteComplete(String workerId, List<String> outputFiles) {
        throw new UnsupportedOperationException("Worker should not receive file write complete");
    }

    private void startHeartBeating() {
        log.info("Starting heartbeat for worker {}", id);
        workerExecutor.scheduleAtFixedRate(() -> workerClient.sendHeartbeat(id, status),
            0, 5, TimeUnit.SECONDS);
    }

    public void sendFileWriteCompleted(List<String> outputFiles) {
        workerClient.sendFileWriteComplete(id, outputFiles);
    }

    @Override
    protected boolean onTaskArrive(String workerId, TaskInfo taskInfo) {
        if (!Objects.equals(workerId, id)) {
            log.error("Received task for another worker - receive id: {} worker id: {}", workerId, id);
            return false;
        }

        if (status != WorkerStatus.IDLE) {
            log.warn("Received task while worker not idle - worker id: {}, status: {}", id, status);
            taskQueue.add(taskInfo);
            return true;
        }

        if (taskInfo.getTaskType() == TaskType.TERMINATE) {
            log.info("Received terminate task - worker id: {}", id);
            status = WorkerStatus.TERMINATED;
            terminateLatch.countDown();
            return true;
        }

        try (Task task = Task.createTask(taskInfo, ctx)) {
            status = WorkerStatus.BUSY;
            task.setAfterExecute((info) -> {
                sendFileWriteCompleted(info.getOutputFiles());
                status = WorkerStatus.IDLE;
                nextTask();
            });
            task.execute();
        }
        return true;
    }

    private void nextTask() {
        TaskInfo taskInfo = taskQueue.poll();
        log.info("Queued task: {}", taskInfo);
        if (taskInfo != null) {
            onTaskArrive(id, taskInfo);
        }
    }
}
