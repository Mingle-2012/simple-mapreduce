package site.pdli.mapreduce.task;

import site.pdli.mapreduce.messaging.Worker.TaskType;

import java.util.List;

public class TaskInfo {
    private final String taskId;
    private final TaskType taskType;
    private final List<String> inputFiles;
    private List<String> outputFiles;

    public TaskInfo(String taskId, TaskType taskType, List<String> inputFiles) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.inputFiles = inputFiles;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    public List<String> getOutputFiles() {
        return outputFiles;
    }

    public void setOutputFiles(List<String> outputFiles) {
        this.outputFiles = outputFiles;
    }
}
