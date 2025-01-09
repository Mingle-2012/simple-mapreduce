package site.pdli;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.pdli.common.Tuple;
import site.pdli.example.WordCountMapper;
import site.pdli.example.WordCountReducer;
import site.pdli.utils.FileUtil;
import site.pdli.worker.Master;
import site.pdli.worker.Worker;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LocalTest {
    @Before
    public void setUp() throws IOException {
        var content = """
            This is a line of text
            This is another line of text
            This is yet another line of text
            """;
        FileUtil.writeLocal("input.txt", content.getBytes());

        Config config = Config.getInstance();
        config.setInputFile(new File("input.txt"));
        config.setNumMappers(2);
        config.setNumReducers(2);
        config.setTmpDir(new File("tmp"));
        config.setOutputDir(new File("out"));
        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);
        config.setSplitMethod(Config.SplitMethod.BY_NUM_MAPPERS);
    }

    @After
    public void tearDown() throws IOException {
        FileUtil.del("input.txt");
        FileUtil.del("tmp");
        FileUtil.del("out");
    }

    @Test
    public void testLocal() throws IOException, InterruptedException {
        Master master = new Master("master", 5000);

        var masterHost = master.getHost();

        master.addWorker("localhost", 5001);
        master.addWorker("localhost", 5002);
        master.addWorker("localhost", 5003);
        master.addWorker("localhost", 5004);

        Worker mapper1 = new Worker("mapper-0", 5001, masterHost, 5000);
        Worker mapper2 = new Worker("mapper-1", 5002, masterHost, 5000);
        Worker reducer1 = new Worker("reducer-0", 5003, masterHost, 5000);
        Worker reducer2 = new Worker("reducer-1", 5004, masterHost, 5000);

        master.start();
        mapper1.start();
        mapper2.start();
        reducer1.start();
        reducer2.start();

        master.splitInput();
        master.assignMapTask();

        var t1 = new Thread(mapper1::block);
        var t2 = new Thread(mapper2::block);
        var t3 = new Thread(reducer1::block);
        var t4 = new Thread(reducer2::block);
        var t5 = new Thread(master::block);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();

        mapper1.close();
        mapper2.close();
        reducer1.close();
        reducer2.close();
        master.close();

        var outDir = Config.getInstance()
            .getOutputDir();
        var files = outDir.listFiles();

        Assert.assertNotNull(files);

        Map<String, Integer> map1 = new HashMap<>();
        Map<String, Integer> map2 = new HashMap<>();

        Arrays.stream(files)
            .map(File::getPath)
            .forEach(p -> {
                try {
                    FileUtil.readLocal(p)
                        .lines()
                        .map(Tuple::fromFileString)
                        .forEach(t -> map1.put(t.key(), Integer.parseInt(t.value())));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

        FileUtil.readLocal("input.txt")
            .lines()
            .flatMap(s -> Arrays.stream(s.split(" ")))
            .forEach(w -> {
                map2.putIfAbsent(w, 0);
                map2.put(w, map2.get(w) + 1);
            });

        Assert.assertEquals(map2, map1);
    }

    @Test public void testLocalNetworking() throws IOException, InterruptedException {
        Config.getInstance().setUsingLocalFileSystemForLocalhost(false);
        testLocal();
    }
}
