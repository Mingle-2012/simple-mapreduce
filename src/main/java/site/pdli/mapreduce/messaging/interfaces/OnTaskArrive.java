package site.pdli.mapreduce.messaging.interfaces;

import site.pdli.mapreduce.task.TaskInfo;

@FunctionalInterface
public interface OnTaskArrive {
    boolean call(String workerId, TaskInfo taskInfo);
}
