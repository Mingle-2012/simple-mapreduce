package site.pdli.worker;

public class WorkerContext {
    private final String host;
    private final int port;
    private Object context;

    public WorkerContext(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Object getContext() {
        return context;
    }

    public void setContext(Object context) {
        this.context = context;
    }
}
