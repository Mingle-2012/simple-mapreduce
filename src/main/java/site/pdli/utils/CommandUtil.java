package site.pdli.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;

public class CommandUtil {
    private CommandUtil() {
    }

    private static final Logger log = LoggerFactory.getLogger(CommandUtil.class);

    public record ExecResult(int code, String output) {
    }

    public static int exec(String command) {
        var processBuilder = new ProcessBuilder("bash", "-c", command);
        log.info("exec command: {}", command);
        processBuilder.redirectErrorStream(true);
        try {
            var process = processBuilder.start();
            try (var reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info(line);
                }
            }
            return process.waitFor();
        } catch (Exception e) {
            log.error("exec command error", e);
            return -1;
        }
    }
}
