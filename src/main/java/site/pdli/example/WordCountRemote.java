package site.pdli.example;

import site.pdli.Config;
import site.pdli.runner.LocalRunner;
import site.pdli.runner.RemoteRunner;
import site.pdli.runner.Runner;

import java.io.File;

public class WordCountRemote {
    public static void main(String[] args) {
        var config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);
        config.setMainClass(WordCountRemote.class);
        config.setMasterPort(10000);

        config.addWorker("node2", 10000);
        config.addWorker("node3", 10000);
        config.setUsingLocalFileSystemForLocalhost(false);
        config.setJarPath("/root/simple-mapreduce-1.0-SNAPSHOT.jar");

        config.setNumReducers(1);
        config.setSplitChunkSize(100 * 1000);

        config.setInputFile(new File("input.txt"));
        config.setOutputDir(new File("out"));

        Runner runner = new RemoteRunner(args);

        runner.run();
        runner.waitForCompletion();
    }
}
