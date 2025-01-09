package site.pdli.mapreduce.utils;

import site.pdli.mapreduce.Config;
import site.pdli.mapreduce.messaging.FileClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileUtil {
    private FileUtil() {
    }

    private static boolean canForwardToLocal(String host) {
        return NetWorkUtil.isLocalhost(host) && Config.getInstance().isUsingLocalFileSystemForLocalhost();
    }

    private static void throwIfIllegalFormat(String file) {
        if (!file.contains("://")) {
            throw new IllegalArgumentException("Invalid file format - file: " + file);
        }
    }

    public static String getHostName(String file) {
        throwIfIllegalFormat(file);
        return file.split("://")[0].split(":")[0];
    }

    public static int getPort(String file) {
        throwIfIllegalFormat(file);
        return Integer.parseInt(file.split("://")[0].split(":")[1]);
    }

    public static String getFileName(String file) {
        throwIfIllegalFormat(file);
        return file.split("://")[1];
    }

    public static String makeFile(String host, int port, String fileName) {
        return host + ":" + port + "://" + fileName;
    }

    public static void writeLocal(String fileName, byte[] data) throws IOException {
        ensureExists(fileName);
        Files.write(Paths.get(fileName), data);
    }

    public static void appendLocal(String fileName, byte[] data) throws IOException {
        ensureExists(fileName);
        Files.write(Paths.get(fileName), data, StandardOpenOption.APPEND);
    }

    public static String readLocal(String fileName) throws IOException {
        return new String(Files.readAllBytes(Paths.get(fileName)));
    }

    public static void writeRemote(String file, byte[] data) {
        var host = getHostName(file);
        var port = getPort(file);
        var fileName = getFileName(file);

        if (canForwardToLocal(host)) {
            try {
                writeLocal(fileName, data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        try (var client = new FileClient(host, port)) {
            client.writeFile(fileName, data);
        }
    }

    public static String readRemote(String file) {
        var host = getHostName(file);
        var port = getPort(file);
        var fileName = getFileName(file);

        if (canForwardToLocal(host)) {
            try {
                return readLocal(fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try (var client = new FileClient(host, port)) {
            var data = client.readFile(fileName);
            return data.toStringUtf8();
        }
    }

    public static void ensureExists(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        var parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

    public static void del(String file) throws IOException {
        if (!new File(file).exists()) {
            return;
        }
        try (var stream = Files.walk(Paths.get(file))) {
            stream.map(Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .forEach(p -> {
                    var ignore = p.delete();
                });
        }
    }
}
