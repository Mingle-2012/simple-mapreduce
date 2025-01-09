package site.pdli.example;

import site.pdli.Context;
import site.pdli.Reducer;

import java.util.List;

public class WordCountReducer implements Reducer<String, String, String, Integer> {
    @Override
    public void reduce(String key, List<String> values, Context<String, Integer> context) {
        context.emit(key, values.stream().mapToInt(Integer::parseInt).sum());
    }
}
