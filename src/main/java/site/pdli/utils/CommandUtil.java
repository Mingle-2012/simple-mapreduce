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

    public static ExecResult exec(String command) {
        try {
            var process = Runtime.getRuntime().exec(command);
            int code = process.waitFor();

            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append("\n");
            }

            log.info("Command: {}\nOutput: {}", command, builder);
            return new ExecResult(code, builder.toString());
        } catch (Exception e) {
            log.error("Error while executing command: {}", command, e);
            throw new RuntimeException(e);
        }
    }
}
