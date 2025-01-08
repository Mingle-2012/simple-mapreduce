package site.pdli;

import java.io.File;

public class Config {
    private Class<Mapper<?, ?, ?, ?>> mapperClass;
    private Class<Reducer<?, ?, ?, ?>> reducerClass;
    private File inputFile;
    private File outputDir;

    private static Config instance;

    public static synchronized Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public Class<Mapper<?, ?, ?, ?>> getMapperClass() {
        return mapperClass;
    }

    public void setMapperClass(Class<Mapper<?, ?, ?, ?>> mapperClass) {
        this.mapperClass = mapperClass;
    }

    public Class<Reducer<?, ?, ?, ?>> getReducerClass() {
        return reducerClass;
    }

    public void setReducerClass(Class<Reducer<?, ?, ?, ?>> reducerClass) {
        this.reducerClass = reducerClass;
    }

    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(File outputDir) {
        this.outputDir = outputDir;
    }

    private Config() {
    }
}
