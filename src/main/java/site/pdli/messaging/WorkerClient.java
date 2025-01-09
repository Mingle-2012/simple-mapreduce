package site.pdli.messaging;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.task.TaskInfo;

import java.util.List;

public class WorkerClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;

    private final Logger log = LoggerFactory.getLogger(WorkerClient.class);

    public WorkerClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
    }

    public void sendHeartbeat(String workerId, Worker.WorkerStatus status) {
        Worker.HeartbeatRequest request = Worker.HeartbeatRequest.newBuilder()
                .setWorkerId(workerId)
                .setStatus(status)
                .build();
        var ignore = blockingStub.heartbeat(request);
        log.trace("Heartbeat sent for worker {}", workerId);
    }

    public void sendFileWriteComplete(String workerId, List<String> outputFiles) {
        Worker.FileWriteCompleteRequest request = Worker.FileWriteCompleteRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllOutputFiles(outputFiles)
                .build();
        var ignore = blockingStub.fileWriteComplete(request);
        log.info("Completed sent for worker {}", workerId);
    }

    public void sendTask(String workerId, TaskInfo taskInfo) {
        Worker.SendTaskRequest request = Worker.SendTaskRequest.newBuilder()
                .setWorkerId(workerId)
                .setTaskId(taskInfo.getTaskId())
                .setTaskType(taskInfo.getTaskType())
                .addAllInputFiles(taskInfo.getInputFiles())
                .build();
        var response = blockingStub.sendTask(request);
        if (!response.getOk()) {
            log.error("Error sending task to worker {}", workerId);
            int retry = 5;
            int maxDelay = 2000;
            while (!response.getOk() && retry > 0) {
                try {
                    Thread.sleep(maxDelay / 5);
                } catch (InterruptedException e) {
                    log.error("Error sleeping", e);
                }
                log.warn("Retrying task for worker {}, tries remain: {}", workerId, retry);
                response = blockingStub.sendTask(request);
                retry--;
            }
        }
        log.info("Task sent for worker {}, ok: {}", workerId, response.getOk());
    }

    @Override
    public void close() {
        log.trace("Closing WorkerClient");
        channel.shutdown();
    }
}
