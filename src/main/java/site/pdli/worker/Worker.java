package site.pdli.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.messaging.HeartbeatInfo;
import site.pdli.messaging.MasterClient;
import site.pdli.messaging.WorkerStatus;
import site.pdli.task.TaskInfo;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Worker implements Runnable {
    protected final String workerId;
    protected final String host;
    protected final int port;
    protected final MasterClient masterClient;
    protected volatile WorkerStatus status;
    protected volatile TaskInfo currentTask;
    protected final ExecutorService executor;
    protected final ScheduledExecutorService heartbeatExecutor;
    protected final BlockingQueue<TaskInfo> taskQueue;
    protected final Map<String, Object> metrics;
    protected final ReentrantLock stateLock;

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    protected Worker(String host, int port, MasterClient masterClient) {
        this.workerId = UUID.randomUUID().toString();
        this.host = host;
        this.port = port;
        this.masterClient = masterClient;
        this.status = WorkerStatus.IDLE;
        this.executor = Executors.newFixedThreadPool(2);
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.taskQueue = new LinkedBlockingQueue<>();
        this.metrics = new ConcurrentHashMap<>();
        this.stateLock = new ReentrantLock();
    }

    // 初始化Worker
    public void initialize() {
        // 注册Worker到Master
        registerWithMaster();
        // 启动心跳
        startHeartbeat();
        // 启动任务处理线程
        executor.submit(this);
    }

    // 向Master注册
    protected void registerWithMaster() {
        try {
            masterClient.registerWorker(workerId, host, port);
            log.info("Worker {} registered with master", workerId);
        } catch (Exception e) {
            log.error("Failed to register worker with master", e);
            throw new RuntimeException("Worker registration failed", e);
        }
    }

    // 启动心跳
    protected void startHeartbeat() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeat();
            } catch (Exception e) {
                log.error("Failed to send heartbeat", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    // 发送心跳
    protected void sendHeartbeat() {
        HeartbeatInfo heartbeat = new HeartbeatInfo(
            workerId,
            status,
            currentTask != null ? currentTask.getTaskId() : null,
            metrics
        );
        masterClient.heartbeat(heartbeat);
    }

    // 任务处理主循环
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TaskInfo task = taskQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    processTask(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error processing task", e);
                failTask(e);
            }
        }
    }

    // 接收新任务
    public void assignTask(TaskInfo task) {
        try {
            stateLock.lock();
            if (status == WorkerStatus.IDLE) {
                taskQueue.offer(task);
                status = WorkerStatus.RUNNING;
                log.info("Worker {} assigned task {}", workerId, task.getTaskId());
            } else {
                log.warn("Worker {} is busy, cannot accept task {}", workerId, task.getTaskId());
            }
        } finally {
            stateLock.unlock();
        }
    }

    // 处理任务
    protected void processTask(TaskInfo task) {
        try {
            currentTask = task;
//            updateStatus(WorkerStatus.RUNNING);

            // 执行具体的任务处理
            process(task);

            completeTask();
        } catch (Exception e) {
            failTask(e);
        }
    }

    // 更新Worker状态
//    protected void updateStatus(WorkerStatus newStatus) {
//        try {
//            stateLock.lock();
//            this.status = newStatus;
//            // 通知Master状态变更
//            masterClient.updateWorkerStatus(workerId, newStatus);
//        } finally {
//            stateLock.unlock();
//        }
//    }

    // 任务完成处理
    protected void completeTask() {
        try {
            stateLock.lock();
            status = WorkerStatus.COMPLETED;
            masterClient.reportTaskComplete(workerId, currentTask.getTaskId());
            currentTask = null;
            status = WorkerStatus.IDLE;
        } finally {
            stateLock.unlock();
        }
    }

    // 任务失败处理
    protected void failTask(Exception e) {
        try {
            stateLock.lock();
            status = WorkerStatus.FAILED;
            masterClient.reportTaskFailure(workerId, currentTask.getTaskId(), e);
            currentTask = null;
            status = WorkerStatus.IDLE;
        } finally {
            stateLock.unlock();
        }
    }

    // 更新度量指标
    protected void updateMetrics(String key, Object value) {
        metrics.put(key, value);
    }

    // 清理资源
    public void shutdown() {
        try {
            executor.shutdown();
            heartbeatExecutor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cleanup();
        }
    }

    // 抽象方法，子类必须实现
    protected abstract void process(TaskInfo task) throws Exception;
    protected abstract void cleanup();
}
