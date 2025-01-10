package site.pdli.example;

import site.pdli.utils.TimeUtil;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountBruteforce {

    private static final Logger log = LoggerFactory.getLogger(WordCountBruteforce.class);

    public static void main(String[] args) {
        String inputFilePath = "datasets/random.txt";
        String outputFilePath = "output.txt";

        var time = TimeUtil.getInstance();
        time.start();

        Map<String, Integer> wordCounts = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
//                line = line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
                String[] words = line.split("\\s+");
                for (String word : words) {
                    if (!word.isEmpty()) {

                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading the input file: " + e.getMessage());
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
            System.out.println("Word count result written to " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Error writing to the output file: " + e.getMessage());
        }

        log.info("Time taken: {} s", time.stop());

    }

}

