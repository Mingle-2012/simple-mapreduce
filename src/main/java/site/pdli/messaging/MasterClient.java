package site.pdli.messaging;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MasterClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final MasterServiceGrpc.MasterServiceBlockingStub blockingStub;
    private final MasterServiceGrpc.MasterServiceStub asyncStub;
    private final String masterAddress;
    private final int masterPort;

    private static final Logger log = LoggerFactory.getLogger(MasterClient.class);

    public MasterClient(String masterAddress, int masterPort) {
        this.masterAddress = masterAddress;
        this.masterPort = masterPort;
        this.channel = ManagedChannelBuilder
            .forAddress(masterAddress, masterPort)
            .usePlaintext()  // 在生产环境中应使用TLS
            .enableRetry()   // 启用重试
            .build();

        this.blockingStub = MasterServiceGrpc.newBlockingStub(channel);
        this.asyncStub = MasterServiceGrpc.newStub(channel);
    }

    // Worker注册
    public boolean registerWorker(String workerId, String host, int port) {
        RegisterWorkerRequest request = RegisterWorkerRequest.newBuilder()
            .setWorkerId(workerId)
            .setHost(host)
            .setPort(port)
            .build();

        try {
            RegisterWorkerResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .registerWorker(request);
            return response.getSuccess();
        } catch (StatusRuntimeException e) {
            log.error("Failed to register worker: {}", e.getMessage());
            throw new RuntimeException("Worker registration failed", e);
        }
    }

    // 发送心跳
    public void heartbeat(HeartbeatInfo heartbeatInfo) {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setWorkerId(heartbeatInfo.getWorkerId())
            .setStatus(convertStatus(heartbeatInfo.getStatus()))
            .setCurrentTaskId(heartbeatInfo.getCurrentTaskId())
            .putAllMetrics(convertMetrics(heartbeatInfo.getMetrics()))
            .build();

        asyncStub.heartbeat(request, new StreamObserver<HeartbeatResponse>() {
            @Override
            public void onNext(HeartbeatResponse response) {
                if (!response.getShouldContinue()) {
                    log.warn("Master requested worker to stop");
                    // 处理停止请求
                }
                // 处理待处理的命令
                for (TaskCommand command : response.getPendingCommandsList()) {
                    handleCommand(command);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Heartbeat failed: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                // 心跳完成
            }
        });
    }

    // 报告任务状态
    public void reportTaskStatus(String workerId, String taskId,
                                 TaskStatus status, String errorMessage) {
        TaskStatusRequest request = TaskStatusRequest.newBuilder()
            .setWorkerId(workerId)
            .setTaskId(taskId)
            .setStatus(status)
            .setErrorMessage(errorMessage != null ? errorMessage : "")
            .build();

        try {
            TaskStatusResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .reportTaskStatus(request);

            if (!response.getAcknowledged()) {
                log.warn("Master did not acknowledge task status update");
            }
        } catch (StatusRuntimeException e) {
            log.error("Failed to report task status: {}", e.getMessage());
            throw new RuntimeException("Task status reporting failed", e);
        }
    }

    // 报告Map输出位置
    public void reportMapOutput(String workerId, int mapTaskId,
                                List<MapOutputInfo> locations) {
        MapOutputRequest.Builder requestBuilder = MapOutputRequest.newBuilder()
            .setWorkerId(workerId)
            .setMapTaskId(mapTaskId);

        for (MapOutputInfo location : locations) {
            MapOutputLocation mapLocation = MapOutputLocation.newBuilder()
                .setFilePath(location.getFilePath())
                .setOffset(location.getOffset())
                .setLength(location.getLength())
                .setPartition(location.getPartition())
                .build();

            requestBuilder.addLocations(mapLocation);
        }

        try {
            MapOutputResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .reportMapOutput(requestBuilder.build());

            if (!response.getAcknowledged()) {
                log.warn("Master did not acknowledge map output location update");
            }
        } catch (StatusRuntimeException e) {
            log.error("Failed to report map output locations: {}", e.getMessage());
            throw new RuntimeException("Map output reporting failed", e);
        }
    }

    // 转换Worker状态为proto枚举
    private WorkerStatus convertStatus(WorkerStatus status) {
        switch (status) {
            case IDLE: return WorkerStatus.IDLE;
            case RUNNING: return WorkerStatus.RUNNING;
            case FAILED: return WorkerStatus.FAILED;
            case COMPLETED: return WorkerStatus.COMPLETED;
            default: throw new IllegalArgumentException("Unknown status: " + status);
        }
    }

    // 转换度量数据为proto格式
    private Map<String, String> convertMetrics(Map<String, Object> metrics) {
        Map<String, String> result = new HashMap<>();
        metrics.forEach((key, value) -> result.put(key, value.toString()));
        return result;
    }

    // 处理来自Master的命令
    private void handleCommand(TaskCommand command) {
//        switch (command.getType()) {
//            case "STOP":
//                handleStopCommand(command);
//                break;
//            case "PAUSE":
//                handlePauseCommand(command);
//                break;
//            case "RESUME":
//                handleResumeCommand(command);
//                break;
//            default:
//                log.warn("Unknown command type: {}", command.getType());
//        }
    }

    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void reportTaskComplete(String workerId, String taskId) {
        TaskStatusRequest request = TaskStatusRequest.newBuilder()
            .setWorkerId(workerId)
            .setTaskId(taskId)
            .setStatus(TaskStatus.TASK_COMPLETED)
            .build();

        try {
            TaskStatusResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .reportTaskStatus(request);

            if (!response.getAcknowledged()) {
                log.warn("Master did not acknowledge task completion");
            }
        } catch (StatusRuntimeException e) {
            log.error("Failed to report task completion: {}", e.getMessage());
            throw new RuntimeException("Task completion reporting failed", e);
        }
    }

    public void reportTaskFailure(String workerId, String taskId, Exception e) {
        TaskStatusRequest request = TaskStatusRequest.newBuilder()
            .setWorkerId(workerId)
            .setTaskId(taskId)
            .setStatus(TaskStatus.TASK_FAILED)
            .setErrorMessage(e.getMessage())
            .build();

        try {
            TaskStatusResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .reportTaskStatus(request);

            if (!response.getAcknowledged()) {
                log.warn("Master did not acknowledge task failure");
            }
        } catch (StatusRuntimeException ex) {
            log.error("Failed to report task failure: {}", ex.getMessage());
            throw new RuntimeException("Task failure reporting failed", ex);
        }
    }
}
