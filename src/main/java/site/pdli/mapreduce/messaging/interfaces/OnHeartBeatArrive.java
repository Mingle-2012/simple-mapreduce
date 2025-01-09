package site.pdli.mapreduce.messaging.interfaces;

import site.pdli.mapreduce.messaging.Worker;

@FunctionalInterface
public interface OnHeartBeatArrive {
    void call(String workerId, Worker.WorkerStatus status);
}
