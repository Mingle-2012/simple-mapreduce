package site.pdli.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.worker.WorkerContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class Task implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Task.class);
    protected TaskInfo taskInfo;
    protected Consumer<TaskInfo> afterExecute;
    protected ExecutorService executorService = Executors.newFixedThreadPool(5);
    protected final String host;
    protected final int port;

    public Task(TaskInfo taskInfo, WorkerContext ctx) {
        this.taskInfo = taskInfo;
        this.host = ctx.getHost();
        this.port = ctx.getPort();
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setAfterExecute(Consumer<TaskInfo> afterExecute) {
        this.afterExecute = afterExecute;
    }

    public void execute() {
        var future = executorService.submit(() -> {
            log.info("Executing task {}", taskInfo.getTaskId());
            doExecute();
            log.info("Task {} completed", taskInfo.getTaskId());
            if (afterExecute != null) {
                log.trace("After execute task {}", taskInfo.getTaskId());
                afterExecute.accept(taskInfo);
                log.trace("After execute task {} completed", taskInfo.getTaskId());
            }
        });

        try {
            future.get();
        } catch (Throwable e) {
            log.error("Error executing task {}", taskInfo.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    protected abstract void doExecute();

    public void close() {
        executorService.shutdown();
    }

    public void join() {
        try {
            close();
            var ignore = executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            log.error("Task {} Interrupted", taskInfo.getTaskId(), e);
        }
    }

    public static Task createTask(TaskInfo taskInfo, WorkerContext ctx) {
        return switch (taskInfo.getTaskType()) {
            case MAP -> new MapTask(taskInfo, ctx);
            case REDUCE, REDUCE_READ -> new ReduceTask(taskInfo, ctx);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskInfo.getTaskType());
        };
    }
}
