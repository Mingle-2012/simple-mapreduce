package site.pdli.mapreduce.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;

public class CommandUtil {
    private CommandUtil() {
    }

    private static final Logger log = LoggerFactory.getLogger(CommandUtil.class);

    public record ExecResult(int code, String output) {
    }

    public static ExecResult exec(String command) {
        var processBuilder = new ProcessBuilder("bash", "-c", command);
        log.info("exec command: {}", command);
        processBuilder.redirectErrorStream(true);
        try {
            var process = processBuilder.start();
            var result = new StringBuilder();
            try (var reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info(line);
                    result.append(line).append("\n");
                }
            }
            return new ExecResult(process.waitFor(), result.toString());
        } catch (Exception e) {
            log.error("exec command error", e);
            return new ExecResult(-1, e.getMessage());
        }
    }
}
