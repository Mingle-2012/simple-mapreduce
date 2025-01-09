package site.pdli.example;

import site.pdli.Config;
import site.pdli.runner.LocalRunner;
import site.pdli.runner.Runner;

import java.io.File;

public class WordCount {
    public static void main(String[] args) {
        var config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);

        config.setMasterPort(5000);
        config.addWorker("localhost", 5001);
        config.addWorker("localhost", 5002);
        config.addWorker("localhost", 5003);
        config.addWorker("localhost", 5004);

        config.setNumReducers(2);

        config.setInputFile(new File("input.txt"));
        config.setOutputDir(new File("out"));

        Runner runner = new LocalRunner();

        runner.run();
        runner.waitForCompletion();
    }
}
