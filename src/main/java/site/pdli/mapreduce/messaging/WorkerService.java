package site.pdli.mapreduce.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.mapreduce.messaging.interfaces.OnHeartBeatArrive;
import site.pdli.mapreduce.messaging.interfaces.OnTaskArrive;
import site.pdli.mapreduce.messaging.interfaces.OnWorkerFileWriteComplete;
import site.pdli.mapreduce.task.TaskInfo;

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
    }

    @Override
    public void fileWriteComplete(Worker.FileWriteCompleteRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        onWorkerFileWriteComplete.call(request.getWorkerId(), request.getOutputFilesList());
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
