package site.pdli.example;

import site.pdli.Context;
import site.pdli.Mapper;

public class WordCountMapper implements Mapper<Integer, String, String, Integer> {
    @Override
    public void map(Integer key, String value, Context<String, Integer> context) {
        for (String word : value.split("\\s+")) {
            context.emit(word, 1);
        }
    }
}
