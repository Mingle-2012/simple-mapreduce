package site.pdli.messaging.interfaces;

import site.pdli.task.TaskInfo;

@FunctionalInterface
public interface OnTaskArrive {
    boolean call(String workerId, TaskInfo taskInfo);
}
