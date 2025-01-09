package site.pdli.mapreduce.example;

import site.pdli.mapreduce.Context;
import site.pdli.mapreduce.Reducer;

import java.util.List;

public class WordCountReducer implements Reducer<String, String, String, Integer> {
    @Override
    public void reduce(String key, List<String> values, Context<String, Integer> context) {
        context.emit(key, values.stream().mapToInt(Integer::parseInt).sum());
    }
}
