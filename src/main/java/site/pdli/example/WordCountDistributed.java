package site.pdli.example;

import site.pdli.Config;
import site.pdli.runner.DistributedRunner;
import site.pdli.runner.Runner;

import java.io.File;

public class WordCountDistributed {
    public static void main(String[] args) {
        var config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);
        config.setMainClass(WordCountDistributed.class);
        config.setMasterPort(10000);

        config.addWorker("node2", 10000);
        config.addWorker("node3", 10000);
        config.setUsingLocalFileSystemForLocalhost(false);
        config.setJarPath("simple-mapreduce-1.0-SNAPSHOT.jar");

        config.setNumReducers(1);
        config.setSplitChunkSize(100 * 1000);

        config.setInputFile(new File("input.txt"));
        config.setOutputDir(new File("out"));

        Runner runner = new DistributedRunner(args);

        runner.run();
        runner.waitForCompletion();
    }
}
