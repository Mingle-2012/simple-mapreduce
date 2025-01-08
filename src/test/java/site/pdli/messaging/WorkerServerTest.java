package site.pdli.messaging;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerServerTest {
    private Server server;

    private final Map<String, Worker.WorkerStatus> workerStatusMap = new HashMap<>();
    private final Map<String, List<String>> workerOutputFilesMap = new HashMap<>();

    @Before
    public void setUp() {
        server = ServerBuilder.forPort(50001)
            .addService(
                new WorkerService(
                    workerStatusMap::put,
                    workerOutputFilesMap::put))
            .build();

        new Thread(() -> {
            try {
                server.start();
                server.awaitTermination();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }).start();

        System.out.println("Server started");
    }

    @After
    public void tearDown() {
        server.shutdown();

        System.out.println("Server stopped");
    }

    @Test
    public void sendHeartbeatTest() {
        WorkerClient client1 = new WorkerClient("localhost", 50001);
        client1.sendHeartbeat("worker1", Worker.WorkerStatus.IDLE);

        WorkerClient client2 = new WorkerClient("localhost", 50001);
        client2.sendHeartbeat("worker2", Worker.WorkerStatus.RUNNING);

        assert workerStatusMap.get("worker1") == Worker.WorkerStatus.IDLE;
        assert workerStatusMap.get("worker2") == Worker.WorkerStatus.RUNNING;
    }

    @Test
    public void sendCompletedTest() {
        WorkerClient client1 = new WorkerClient("localhost", 50001);
        client1.sendCompleted("worker1", List.of("output1.txt", "output2.txt"));

        WorkerClient client2 = new WorkerClient("localhost", 50001);
        client2.sendCompleted("worker2", List.of("output3.txt", "output4.txt"));

        assert workerOutputFilesMap.get("worker1").equals(List.of("output1.txt", "output2.txt"));
        assert workerOutputFilesMap.get("worker2").equals(List.of("output3.txt", "output4.txt"));
    }
}
