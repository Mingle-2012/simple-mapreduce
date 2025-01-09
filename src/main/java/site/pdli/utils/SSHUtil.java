package site.pdli.utils;

import org.slf4j.Logger;

import java.util.Objects;

public class SSHUtil {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(SSHUtil.class);

    public static boolean checkHost(String host) {
        var res = CommandUtil.exec("ssh -o BatchMode=yes -o ConnectTimeout=5 " + host + " echo ok");
        return Objects.equals(res.output(), "ok");
    }

    public static void copyToRemote(String host, String localFile, String remoteFile) {
        CommandUtil.exec("scp " + localFile + " " + host + ":" + remoteFile);
        log.info("Copy {} to {}:{} successfully", localFile, host, remoteFile);
    }

    public static void execCommandOnRemote(String host, String command) {
        command = "\"" + command + " > /dev/null 2>&1 &" + "\"";
        CommandUtil.exec("ssh " + host + " " + command);
        log.info("Exec command {} on {} successfully", command, host);
    }
}
