package site.pdli.worker;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WorkerTest {
    private Worker worker1, worker2;
    private Master master;

    @Before
    public void setUp() {
        master = new Master("master", 5000);
        var masterHost = master.getHost();
        var port = master.getPort();

        worker1 = new Worker("worker-1", 50001, masterHost, port);
        worker2 = new Worker("worker-2", 50002, masterHost, port);

        master.start();
        worker1.start();
        worker2.start();

        System.out.println("Setup complete");
        System.out.println("Master host: " + masterHost);
    }

    @After
    public void tearDown() {
        worker1.close();
        worker2.close();
        master.close();

        System.out.println("Teardown complete");
    }


    @Test
    public void testWorker() throws InterruptedException {
        Thread.sleep(6000);
    }
}
