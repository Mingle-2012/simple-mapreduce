package site.pdli.mapreduce.messaging;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

public class FileServerTest {
    private Server server;

    @Before
    public void setUp() {
        server = ServerBuilder.forPort(50001)
            .addService(new FileService())
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

        try {
            Files.delete(Paths.get("test.txt"));
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    @Test
    public void testReadFile() throws Exception {
        Files.write(Paths.get("test.txt"), "Hello, World!".getBytes());
        try (FileClient client = new FileClient("localhost", 50001)) {
            var content = client.readFile("test.txt");
            assert content.toStringUtf8().equals("Hello, World!");
        }
    }

    @Test
    public void testWriteFile() throws Exception {
        try (FileClient client = new FileClient("localhost", 50001)) {
            client.writeFile("test.txt", "Hello, World!".getBytes());
        }
        var content = Files.readString(Paths.get("test.txt"));
        assert content.equals("Hello, World!");
    }
}
