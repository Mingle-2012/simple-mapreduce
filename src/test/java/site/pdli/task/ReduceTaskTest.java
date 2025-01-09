package site.pdli.task;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.pdli.Config;
import site.pdli.example.WordCountMapper;
import site.pdli.example.WordCountReducer;
import site.pdli.messaging.Worker;
import site.pdli.utils.FileUtil;
import site.pdli.worker.WorkerContext;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ReduceTaskTest {
    @Before
    public void setUp() throws IOException {
        Config config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);
        config.setNumMappers(1);
        config.setNumReducers(2);
        config.setTmpDir(new File("tmp"));
        config.setOutputDir(new File("out"));

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
        FileUtil.del("out");
    }

    @Test
    public void testReduceTask1() {
        List<String> outputFiles;

        try (var maptask = Task.createTask(
            new TaskInfo("mapper-1", Worker.TaskType.MAP,
                List.of("localhost:12345://input1")),
            new WorkerContext("localhost", 10000))) {
            maptask.execute();
            maptask.join();
            outputFiles = maptask.getTaskInfo()
                .getOutputFiles();
        }

        var ctx = new WorkerContext("localhost", 10005);

        try (var reduceTask = Task.createTask(
            new TaskInfo("reducer-1", Worker.TaskType.REDUCE_READ, outputFiles), ctx)) {
            reduceTask.execute();
            reduceTask.join();
        }

        try (var mapTask2 = Task.createTask(
            new TaskInfo("mapper-2", Worker.TaskType.MAP,
                List.of("localhost:12345://input2")),
            new WorkerContext("localhost", 10001))) {
            mapTask2.execute();
            mapTask2.join();
            outputFiles = mapTask2.getTaskInfo()
                .getOutputFiles();
        }

        try (var reduceTask2 = Task.createTask(
            new TaskInfo("reducer-2", Worker.TaskType.REDUCE_READ, outputFiles), ctx)) {
            reduceTask2.execute();
            reduceTask2.join();
        }

        try (var reduceTask3 = Task.createTask(
            new TaskInfo("reducer-3", Worker.TaskType.REDUCE, List.of()), ctx)) {
            reduceTask3.execute();
            reduceTask3.join();
        }

        String part0;
        String part1;

        try {
            part0 = FileUtil.readLocal("out/part-0");
            part1 = FileUtil.readLocal("out/part-1");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals("""
            "haha"|"2"
            "line"|"3"
            "content1"|"1"
            "this"|"2"
            "yet"|"1"
            "is"|"2"
            """, part0);

        Assert.assertEquals("""
            "content2"|"1"
            "another"|"3"
            """, part1);
    }

    @Test
    public void testReduceTask2() {
        List<String> outputFiles;

        try (var maptask = Task.createTask(
            new TaskInfo("mapper-1", Worker.TaskType.MAP,
                List.of("localhost:12345://input1", "localhost:12345://input2")),
            new WorkerContext("localhost", 10000))) {
            maptask.execute();
            maptask.join();
            outputFiles = maptask.getTaskInfo()
                .getOutputFiles();
        }

        var ctx1 = new WorkerContext("localhost", 10005);

        try (var reduceTask11 = Task.createTask(
            new TaskInfo("reducer-1-1", Worker.TaskType.REDUCE_READ, outputFiles.stream()
                .filter(f -> f.contains("part-0"))
                .toList()), ctx1)) {
            reduceTask11.execute();
            reduceTask11.join();
        }

        try (var reduceTask12 = Task.createTask(
            new TaskInfo("reducer-1-2", Worker.TaskType.REDUCE, List.of()), ctx1)) {
            reduceTask12.execute();
            reduceTask12.join();
        }

        var ctx2 = new WorkerContext("localhost", 10006);

        try (var reduceTask21 = Task.createTask(
            new TaskInfo("reducer-2-1", Worker.TaskType.REDUCE_READ, outputFiles.stream()
                .filter(f -> f.contains("part-1"))
                .toList()), ctx2)) {
            reduceTask21.execute();
            reduceTask21.join();
        }

        try (var reduceTask22 = Task.createTask(
            new TaskInfo("reducer-2-2", Worker.TaskType.REDUCE, List.of()), ctx2)) {
            reduceTask22.execute();
            reduceTask22.join();
        }

        String part0;
        String part1;

        try {
            part0 = FileUtil.readLocal("out/part-0");
            part1 = FileUtil.readLocal("out/part-1");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals("""
            "haha"|"2"
            "line"|"3"
            "content1"|"1"
            "this"|"2"
            "yet"|"1"
            "is"|"2"
            """, part0);

        Assert.assertEquals("""
            "content2"|"1"
            "another"|"3"
            """, part1);
    }
}
