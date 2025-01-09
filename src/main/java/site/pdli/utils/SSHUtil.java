package site.pdli.utils;

public class SSHUtil {
    public static boolean checkHost(String host) {
        var res = CommandUtil.exec("ssh -o BatchMode=yes -o ConnectTimeout=5 " + host + " echo ok");
        return res.code() == 0;
    }

    public static void copyToRemote(String host, String localFile, String remoteFile) {
        CommandUtil.exec("scp " + localFile + " " + host + ":" + remoteFile);
    }

    public static void execCommandOnRemote(String host, String command) {
        CommandUtil.exec("ssh " + host + " " + command);
    }
}
