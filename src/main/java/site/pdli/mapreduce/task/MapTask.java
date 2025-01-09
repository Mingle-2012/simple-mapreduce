package site.pdli.mapreduce.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.pdli.mapreduce.Config;
import site.pdli.mapreduce.Mapper;
import site.pdli.mapreduce.common.ContextImpl;
import site.pdli.mapreduce.common.partitioner.HashcodePartitioner;
import site.pdli.mapreduce.utils.FileUtil;
import site.pdli.mapreduce.worker.WorkerContext;

public class MapTask extends Task {
    private final Mapper<Object, Object, Object, Object> mapper;

    private static final Logger log = LoggerFactory.getLogger(MapTask.class);

    @SuppressWarnings("unchecked")
    public MapTask(TaskInfo taskInfo, WorkerContext ctx) {
        super(taskInfo, ctx);

        Class<? extends Mapper<?, ?, ?, ?>> mapperClass = Config.getInstance()
            .getMapperClass();
        try {
            mapper = (Mapper<Object, Object, Object, Object>) mapperClass.getDeclaredConstructor()
                .newInstance();
        } catch (Exception e) {
            log.error("Error creating mapper", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doExecute() {
        var inputFiles = taskInfo.getInputFiles();

        var tmpDir = Config.getInstance()
            .getTmpDir()
            .getPath();

        var context = new ContextImpl<>(new HashcodePartitioner(), tmpDir + "/" + host + "/" + port);
        for (var inputFile : inputFiles) {
            var lines = FileUtil.readRemote(inputFile)
                .lines()
                .toList();

            for (int i = 0; i < lines.size(); i++) {
                var line = lines.get(i);
                mapper.map(i, line, context);
            }
        }
        context.close();

        taskInfo.setOutputFiles(context.getOutputFiles()
            .stream()
            .filter(f -> f != null && !f.isEmpty())
            .map(f -> FileUtil.makeFile(host, port, tmpDir + "/" + host + "/" + port + "/" + f))
            .toList());

    }
}
