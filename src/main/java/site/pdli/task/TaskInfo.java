package site.pdli.task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TaskInfo {
    private final String taskId;
    private final TaskType type;
    private final Map<String, Object> parameters;

    public TaskInfo(String taskId, TaskType type, Map<String, Object> parameters) {
        this.taskId = taskId;
        this.type = type;
        this.parameters = new HashMap<>(parameters);
    }

    // getters
    public String getTaskId() { return taskId; }
    public TaskType getType() { return type; }
    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }
}
