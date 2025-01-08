package site.pdli.messaging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HeartbeatInfo {
    private final String workerId;
    private final WorkerStatus status;
    private final String currentTaskId;
    private final Map<String, Object> metrics;

    public HeartbeatInfo(String workerId, WorkerStatus status,
                         String currentTaskId, Map<String, Object> metrics) {
        this.workerId = workerId;
        this.status = status;
        this.currentTaskId = currentTaskId;
        this.metrics = new HashMap<>(metrics);
    }

    // getters
    public String getWorkerId() { return workerId; }
    public WorkerStatus getStatus() { return status; }
    public String getCurrentTaskId() { return currentTaskId; }
    public Map<String, Object> getMetrics() { return Collections.unmodifiableMap(metrics); }
}
