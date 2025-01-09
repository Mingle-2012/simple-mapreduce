package site.pdli.task;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.pdli.Config;
import site.pdli.example.WordCountMapper;
import site.pdli.messaging.Worker;
import site.pdli.utils.FileUtil;
import site.pdli.worker.WorkerContext;

import java.io.IOException;
import java.util.List;

public class MapTaskTest {
    @Before
    public void setUp() throws IOException {
        Config.getInstance()
            .setMapperClass(WordCountMapper.class);
        Config.getInstance()
            .setNumReducers(2);

        var content1 = "haha this is content1\nanother line";
        var content2 = "haha this is content2\nanother line\nyet another line";

        FileUtil.writeLocal("input1", content1.getBytes());
        FileUtil.writeLocal("input2", content2.getBytes());
    }

    @After
    public void tearDown() throws IOException {
        FileUtil.del("input1");
        FileUtil.del("input2");
        FileUtil.del("tmp");
    }

    @Test
    public void testMapTask() {
        try (var task = Task.createTask(
            new TaskInfo("task1", Worker.TaskType.MAP,
                List.of("localhost:10001://input1", "localhost:10002://input2")),
            new WorkerContext("localhost", 10000))) {
            task.setAfterExecute(this::afterExecute);
            task.execute();
            task.join();
        }
    }

    public void afterExecute(TaskInfo taskInfo) {
        var files = taskInfo.getOutputFiles();

        byte[] part0;
        byte[] part1;

        try {
            part0 = FileUtil.readLocal(FileUtil.getFileName(files.get(0)));
            part1 = FileUtil.readLocal(FileUtil.getFileName(files.get(1)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals("""
            "haha"|"1"
            "this"|"1"
            "is"|"1"
            "content1"|"1"
            "line"|"1"
            "haha"|"1"
            "this"|"1"
            "is"|"1"
            "line"|"1"
            "yet"|"1"
            "line"|"1"
            """, new String(part0));

        Assert.assertEquals("""
            "another"|"1"
            "content2"|"1"
            "another"|"1"
            "another"|"1"
            """, new String(part1));

    }
}
