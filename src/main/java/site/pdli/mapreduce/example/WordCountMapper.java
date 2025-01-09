package site.pdli.mapreduce.example;

import site.pdli.mapreduce.Context;
import site.pdli.mapreduce.Mapper;

public class WordCountMapper implements Mapper<Integer, String, String, Integer> {
    @Override
    public void map(Integer key, String value, Context<String, Integer> context) {
        for (String word : value.split("\\s+")) {
            context.emit(word, 1);
        }
    }
}
