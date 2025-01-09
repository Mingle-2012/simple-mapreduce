package site.pdli.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.Worker.WorkerStatus;
import site.pdli.messaging.WorkerClient;
import site.pdli.task.Task;
import site.pdli.task.TaskInfo;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Worker extends WorkerBase {
    protected String masterHost;
    protected int masterPort;
    protected WorkerStatus status = WorkerStatus.IDLE;

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
        workerExecutor.scheduleAtFixedRate(() -> workerClient.sendHeartbeat(id, status), 0, 5, TimeUnit.SECONDS);
    }

    public void sendFileWriteCompleted(List<String> outputFiles) {
        workerClient.sendFileWriteComplete(id, outputFiles);
    }

    @Override
    protected boolean onTaskArrive(String workerId, TaskInfo taskInfo) {
        if (!Objects.equals(workerId, id) || status != WorkerStatus.IDLE) {
            return false;
        }
        Task task = Task.createTask(taskInfo, ctx);
        status = WorkerStatus.BUSY;
        task.setAfterExecute((info) -> {
            status = WorkerStatus.FINISHED;
            sendFileWriteCompleted(info.getOutputFiles());
        });
        task.execute();
        return true;
    }
}
