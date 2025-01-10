package site.pdli.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.Config;
import site.pdli.runner.LocalRunner;
import site.pdli.runner.Runner;
import site.pdli.utils.FileUtil;
import site.pdli.utils.TimeUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class WordCountExp {

    private static final Logger log = LoggerFactory.getLogger(WordCount.class);

    private static void remake() throws IOException {
        FileUtil.del("tmp");
        FileUtil.del("out");
    }

    private static double run(int worker, int reducer) throws IOException {
        int runs = 5;
        double min_time = Integer.MAX_VALUE;

        int dataset = 5;
        var input = switch (dataset){
            case 2 -> "A_Christmas_Carol";
            case 3 -> "OLIVER_TWIST";
            case 4 -> "King";
            case 5 -> "random";
            default -> throw new IllegalStateException("Unexpected value: " + dataset);
        };
        input  = "datasets/" + input + ".txt";

//        input = "input.txt";

        while(--runs >= 0){
            remake();


            var config = Config.getInstance();

            config.setMapperClass(WordCountMapper.class);
            config.setReducerClass(WordCountReducer.class);

//            config.setMasterPort(12041);
            for(int i = 0; i < worker; i++){
                config.addWorker("localhost");
            }

            config.setNumReducers(reducer);

            config.setInputFile(new File(input));
            config.setOutputDir(new File("out"));
            config.setUsingLocalFileSystemForLocalhost(false);

            Runner runner = new LocalRunner();

            var timer = TimeUtil.getInstance();
            timer.start();

            runner.run();
            runner.waitForCompletion();

            var time = timer.stop();
            min_time = Math.min(min_time, time);
        }

        return min_time;
    }

    public static void main(String[] args) throws IOException {

        int worker = 6;
        int reducer = 5;
        int mapper = worker - reducer;
        double time = run(worker, reducer);

        log.error("Time taken: {} s", time);

//        for(int w = 2 ; w < 8 ; ++w){
//            for(int r = 1; r < w - 1; ++r){
//
//                int m = w - r;
//                try(var out = new FileOutputStream("results.txt", true)){
//                    out.write(String.format("%d %d %f\n", m, r, time).getBytes());
//                }
//            }
//        }

    }

}