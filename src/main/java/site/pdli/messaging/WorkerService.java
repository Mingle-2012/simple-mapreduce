package site.pdli.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.interfaces.OnHeartBeatArrive;
import site.pdli.messaging.interfaces.OnTaskArrive;
import site.pdli.messaging.interfaces.OnWorkerFileWriteComplete;
import site.pdli.task.TaskInfo;

public class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
    private final OnHeartBeatArrive onHeartBeatArrive;
    private final OnWorkerFileWriteComplete onWorkerFileWriteComplete;
    private final OnTaskArrive onTaskArrive;

    private final Logger log = LoggerFactory.getLogger(WorkerService.class);

    public WorkerService(OnHeartBeatArrive onHeartBeatArrive,
                         OnWorkerFileWriteComplete onWorkerFileWriteComplete,
                         OnTaskArrive onTaskArrive) {
        this.onHeartBeatArrive = onHeartBeatArrive;
        this.onWorkerFileWriteComplete = onWorkerFileWriteComplete;
        this.onTaskArrive = onTaskArrive;
    }

    @Override
    public void heartbeat(Worker.HeartbeatRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        onHeartBeatArrive.call(request.getWorkerId(), request.getStatus());

        log.info("Heartbeat received for worker {}", request.getWorkerId());
    }

    @Override
    public void fileWriteComplete(Worker.FileWriteCompleteRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        onWorkerFileWriteComplete.call(request.getWorkerId(), request.getOutputFilesList());

        log.info("FileWriteComplete received for worker {}", request.getWorkerId());
    }

    @Override
    public void sendTask(Worker.SendTaskRequest request, StreamObserver<Worker.SendTaskResponse> responseObserver) {
        log.info("Task received for worker {}", request.getWorkerId());

        var res = onTaskArrive.call(request.getWorkerId(),
            new TaskInfo(request.getTaskId(), request.getTaskType(), request.getInputFilesList()));

        responseObserver.onNext(Worker.SendTaskResponse.newBuilder().setOk(res).build());
        responseObserver.onCompleted();
    }
}
