package site.pdli.mapreduce.common;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import site.pdli.mapreduce.Config;
import site.pdli.mapreduce.common.partitioner.HashcodePartitioner;
import site.pdli.mapreduce.utils.FileUtil;

import java.io.File;
import java.io.IOException;

public class ContextImplTest {
    @Test
    public void testMapperContext() throws InterruptedException, IOException {
        Config config = Config.getInstance();
        config.setNumReducers(2);
        config.setOutputDir(new File("out"));
        ContextImpl<String, Integer> context = new ContextImpl<>(new HashcodePartitioner(), "tmp");
        context.emit("hello", 1);
        context.emit("world", 2);
        context.emit("hello", 3);
        context.emit("world", 4);
        Thread.sleep(6000);

        var part0 = FileUtil.readLocal("tmp/part-0");
        Assert.assertEquals(
            "\"hello\"|\"1\"\n\"world\"|\"2\"\n\"hello\"|\"3\"\n\"world\"|\"4\"\n",
            new String(part0)
        );

        context.emit("abcdefghi", 8);
        context.emit("jklmnopq", 9);
        Thread.sleep(6000);

        part0 = FileUtil.readLocal("tmp/part-0");
        var part1 = FileUtil.readLocal("tmp/part-1");
        Assert.assertEquals(
            "\"hello\"|\"1\"\n\"world\"|\"2\"\n\"hello\"|\"3\"\n\"world\"|\"4\"\n\"jklmnopq\"|\"9\"\n",
            part0
        );
        Assert.assertEquals(
            "\"abcdefghi\"|\"8\"\n",
            part1
        );
        context.close();
    }

    @After
    public void tearDown() throws IOException {
        FileUtil.del("tmp");
    }
}
