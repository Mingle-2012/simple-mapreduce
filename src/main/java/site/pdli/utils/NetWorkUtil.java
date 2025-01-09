package site.pdli.utils;

import java.net.InetAddress;
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
}
