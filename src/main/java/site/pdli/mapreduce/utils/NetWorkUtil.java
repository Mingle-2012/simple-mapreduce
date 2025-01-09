package site.pdli.mapreduce.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

public class NetWorkUtil {
    public static String getLocalhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isLocalhost(String host) {
        return "localhost".equals(host) || getLocalhost().equals(host);
    }

    public static int getRandomIdlePort() {
        try(var serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
