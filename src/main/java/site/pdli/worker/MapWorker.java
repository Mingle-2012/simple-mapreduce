package site.pdli.worker;

import site.pdli.messaging.MasterClient;
import site.pdli.task.TaskInfo;

public class MapWorker extends Worker {
    protected MapWorker(String host, int port, MasterClient masterClient) {
        super(host, port, masterClient);
    }

    @Override
    protected void process(TaskInfo task) throws Exception {

    }

    @Override
    protected void cleanup() {

    }
}
