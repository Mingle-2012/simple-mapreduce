package site.pdli.messaging;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkerClient {
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;

    private final Logger log = LoggerFactory.getLogger(WorkerClient.class);

    public WorkerClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
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
        log.info("Heartbeat sent for worker {}", workerId);
    }

    public void sendCompleted(String workerId, List<String> outputFiles) {
        Worker.CompletedRequest request = Worker.CompletedRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllOutputFiles(outputFiles)
                .build();
        var ignore = blockingStub.completed(request);
        log.info("Completed sent for worker {}", workerId);
    }
}
