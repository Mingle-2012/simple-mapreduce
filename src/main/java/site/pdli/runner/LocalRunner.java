package site.pdli.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.Config;
import site.pdli.worker.Master;
import site.pdli.worker.Worker;
import site.pdli.worker.WorkerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("FieldMayBeFinal")
public class LocalRunner implements Runner {
    private Master master;
    private Config config = Config.getInstance();
    private final List<Worker> workers = new ArrayList<>();
    private Map<WorkerContext, String> workerContextToId = new HashMap<>();

    private Thread masterThread;
    private List<Thread> workerThreads = new ArrayList<>();

    private CountDownLatch masterInitLatch = new CountDownLatch(1);
    private CountDownLatch workersInitLatch = new CountDownLatch(config.getWorkers()
        .size());

    protected static Logger log = LoggerFactory.getLogger(LocalRunner.class);

    protected void startMaster() {
        masterThread = new Thread(() -> {
            try {
                master = new Master("master", config.getMasterPort());

                int i = config.getNumReducers();
                for (var worker : config.getWorkers()) {
                    if (i > 0) {
                        workerContextToId.put(worker, "reducer-" + i);
                        master.addReducer("reducer-" + i--, worker.getHost(), worker.getPort());
                    } else {
                        master.addMapper("mapper-" + -(--i), worker.getHost(), worker.getPort());
                        workerContextToId.put(worker, "mapper-" + -i);
                    }
                }

                master.start();

                masterInitLatch.countDown();

                workersInitLatch.await();

                master.splitInput();
                master.assignMapTask();

                master.block();
            } catch (Exception e) {
                log.error("Error in master", e);
                System.exit(1);
            }
        });

        masterThread.start();
    }

    protected void startWorkers() {
        for (int i = 0; i < config.getWorkers()
            .size(); i++) {
            try {
                masterInitLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            var worker = config.getWorkers()
                .get(i);
            var workerId = workerContextToId.get(worker);
            var workerThread = getWorkerThread(workerId, worker);
            workerThreads.add(workerThread);
        }

        workerThreads.forEach(Thread::start);
    }

    private Thread getWorkerThread(String workerId, WorkerContext worker) {
        var masterHost = master.getHost();
        var masterPort = master.getPort();
        return new Thread(() -> {
            try {
                var w = new Worker(workerId, worker.getPort(), masterHost, masterPort);

                synchronized (workers) {
                    workers.add(w);
                }

                w.start();

                workersInitLatch.countDown();

                w.block();
            } catch (Exception e) {
                log.error("Error in worker", e);
                System.exit(1);
            }
        });
    }

    @Override
    public void run() {
        config.checkForLocal();

        startMaster();
        startWorkers();
    }

    @Override
    public void waitForCompletion() {
        try {
            masterThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        workerThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
