package site.pdli.example;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.pdli.Config;
import site.pdli.common.Tuple;
import site.pdli.utils.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class WordCountExampleTest {
    @Before
    public void setUp() throws IOException {
        var content = """
            Cloud Computing Data
            Center Center Data
            Cloud Center Computing
            """;
        FileUtil.writeLocal("input.txt", content.getBytes());
    }

    @After
    public void tearDown() throws IOException {
        FileUtil.del("input.txt");
        FileUtil.del("out");
    }

    @Test
    public void testWordCountExampleLocal() throws IOException {
        WordCount.main(new String[]{"input.txt", "out"});

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
}
