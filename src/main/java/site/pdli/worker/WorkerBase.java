package site.pdli.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.FileService;
import site.pdli.messaging.Worker;
import site.pdli.messaging.WorkerService;
import site.pdli.task.TaskInfo;
import site.pdli.utils.NetWorkUtil;

public abstract class WorkerBase implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerBase.class);
    protected String id;
    protected String host;
    protected int port;
    protected Server server;
    protected Thread serverThread;

    protected WorkerBase(String id, int port) {
        this.id = id;
        this.port = port;
        this.host = NetWorkUtil.getLocalhost();

        server = ServerBuilder.forPort(port)
            .addService(new FileService())
            .addService(new WorkerService(this::onHeartBeatArrive, this::onWorkerFileWriteComplete, this::onTaskArrive))
            .build();
    }

    public void startServers() {
        serverThread = new Thread(() -> {
            try {
                server.start();
                server.awaitTermination();
            } catch (InterruptedException e) {
                log.info("Server interrupted");
            } catch (Exception e) {
                log.error("Error in server", e);
                throw new RuntimeException(e);
            }
        });
        serverThread.start();
    }

    public void start() {
        startServers();
    }

    @Override
    public void close() {
        serverThread.interrupt();
        server.shutdown();
    }

    protected abstract void onHeartBeatArrive(String workerId, Worker.WorkerStatus status);

    protected abstract void onWorkerFileWriteComplete(String workerId, java.util.List<String> outputFiles);

    protected abstract boolean onTaskArrive(String workerId, TaskInfo taskInfo);
}
