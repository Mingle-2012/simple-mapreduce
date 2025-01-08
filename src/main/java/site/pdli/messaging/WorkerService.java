package site.pdli.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
    private final OnHeartBeatArrive onHeartBeatArrive;

    private final OnWorkerComplete onWorkerComplete;

    private final Logger log = LoggerFactory.getLogger(WorkerService.class);

    public WorkerService(OnHeartBeatArrive onHeartBeatArrive, OnWorkerComplete onWorkerComplete) {
        this.onHeartBeatArrive = onHeartBeatArrive;
        this.onWorkerComplete = onWorkerComplete;
    }

    @Override
    public void heartbeat(Worker.HeartbeatRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        onHeartBeatArrive.onHeartBeatArrive(request.getWorkerId(), request.getStatus());

        log.info("Heartbeat received for worker {}", request.getWorkerId());
    }

    @Override
    public void completed(Worker.CompletedRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        onWorkerComplete.onWorkerComplete(request.getWorkerId(), request.getOutputFilesList());

        log.info("Completed received for worker {}", request.getWorkerId());
    }
}
