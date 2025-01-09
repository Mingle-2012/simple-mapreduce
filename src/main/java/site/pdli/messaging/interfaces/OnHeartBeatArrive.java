package site.pdli.messaging.interfaces;

import site.pdli.messaging.Worker;

@FunctionalInterface
public interface OnHeartBeatArrive {
    void call(String workerId, Worker.WorkerStatus status);
}
