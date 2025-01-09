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
        try {
            var process = Runtime.getRuntime()
                .exec("""
                    bash -c '%s'
                    """.formatted(command));
            int code = process.waitFor();
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line)
                    .append("\n");
            }
            log.info("Command: {}\nOutput: {}", command, output);
            return code;
        } catch (Exception e) {
            log.error("Error while executing command: {}", command, e);
            throw new RuntimeException(e);
        }
    }
}
