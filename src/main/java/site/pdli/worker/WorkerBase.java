package site.pdli.worker;

import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.FileService;
import site.pdli.messaging.Worker;
import site.pdli.messaging.WorkerService;
import site.pdli.task.TaskInfo;

import java.net.InetAddress;

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

        try {
            this.host = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            log.error("Error getting host address", e);
        }

        server = io.grpc.ServerBuilder.forPort(port)
            .addService(new FileService())
            .addService(new WorkerService(this::onHeartBeatArrive, this::onWorkerFileWriteComplete, this::onTaskArrive))
            .build();
    }

    public void startWorkers() {
        serverThread = new Thread(() -> {
            try {
                server.start();
                server.awaitTermination();
            } catch (InterruptedException e) {
                System.out.println("Server interrupted");
            } catch (Exception e) {
                System.out.println("Error in server");
            }
        });
        serverThread.start();
    }

    public void start() {
        startWorkers();
    }

    @Override
    public void close() {
        serverThread.interrupt();
        server.shutdown();
    }

    @SuppressWarnings("unused")
    protected void onHeartBeatArrive(String workerId, Worker.WorkerStatus status) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @SuppressWarnings("unused")
    protected void onWorkerFileWriteComplete(String workerId, java.util.List<String> outputFiles) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @SuppressWarnings("unused")
    protected boolean onTaskArrive(String workerId, TaskInfo taskInfo) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
