package site.pdli.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.Worker.TaskType;
import site.pdli.messaging.Worker.WorkerStatus;
import site.pdli.messaging.WorkerClient;
import site.pdli.task.Task;
import site.pdli.task.TaskInfo;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker extends WorkerBase {
    protected String masterHost;
    protected int masterPort;
    protected AtomicInteger status = new AtomicInteger(WorkerStatus.IDLE.getNumber());

    protected Logger log = LoggerFactory.getLogger(Worker.class);

    protected WorkerClient workerClient;
    protected ScheduledExecutorService workerExecutor = Executors.newScheduledThreadPool(1);

    protected WorkerContext ctx;

    public Worker(String id, int port, String masterHost, int masterPort) {
        super(id, port);
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.workerClient = new WorkerClient(masterHost, masterPort);

        this.ctx = new WorkerContext(host, port);
    }

    public void block() {
        while (status.get() != WorkerStatus.FINISHED.getNumber()) ;

        log.info("Worker {} finished", id);
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
    }

    private void startHeartBeating() {
        log.info("Starting heartbeat for worker {}", id);
        workerExecutor.scheduleAtFixedRate(() -> workerClient.sendHeartbeat(id, WorkerStatus.forNumber(status.get())),
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

        if (status.get() != WorkerStatus.IDLE.getNumber()) {
            log.error("Received task while worker not idle - worker id: {}, status: {}", id, status);
            return false;
        }

        try (Task task = Task.createTask(taskInfo, ctx)) {
            status.set(WorkerStatus.BUSY.getNumber());
            task.setAfterExecute((info) -> {
                sendFileWriteCompleted(info.getOutputFiles());
                if (info.getTaskType() == TaskType.REDUCE_READ) {
                    status.set(WorkerStatus.IDLE.getNumber());
                } else {
                    status.set(WorkerStatus.FINISHED.getNumber());
                }
            });
            task.execute();
        }
        return true;
    }
}
