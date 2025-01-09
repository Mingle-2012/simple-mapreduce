package site.pdli.mapreduce.worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.mapreduce.Config;
import site.pdli.mapreduce.messaging.FileService;
import site.pdli.mapreduce.messaging.Worker;
import site.pdli.mapreduce.messaging.WorkerService;
import site.pdli.mapreduce.task.TaskInfo;
import site.pdli.mapreduce.utils.FileUtil;
import site.pdli.mapreduce.utils.NetWorkUtil;

import java.io.IOException;

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

        try {
            FileUtil.del(Config.getInstance().getTmpDir().getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void onHeartBeatArrive(String workerId, Worker.WorkerStatus status);

    protected abstract void onWorkerFileWriteComplete(String workerId, java.util.List<String> outputFiles);

    protected abstract boolean onTaskArrive(String workerId, TaskInfo taskInfo);
}
