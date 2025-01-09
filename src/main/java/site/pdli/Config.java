package site.pdli;

import java.io.File;

public class Config {
    private Class<? extends Mapper<?, ?, ?, ?>> mapperClass;
    private Class<? extends Reducer<?, ?, ?, ?>> reducerClass;
    private File inputFile;
    private File outputDir;

    private File tmpDir = new File("tmp");
    private int numMappers;
    private int numReducers;

    public boolean isUsingLocalFileSystemForLocalhost() {
        return usingLocalFileSystemForLocalhost;
    }

    public void setUsingLocalFileSystemForLocalhost(boolean usingLocalFileSystemForLocalhost) {
        this.usingLocalFileSystemForLocalhost = usingLocalFileSystemForLocalhost;
    }

    private boolean usingLocalFileSystemForLocalhost = true;

    private static Config instance;

    private Config() {
    }

    public static synchronized Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() {
        throwIfUnset(mapperClass, "mapperClass");
        return mapperClass;
    }

    public void setMapperClass(Class<? extends Mapper<?, ?, ?, ?>> mapperClass) {
        this.mapperClass = mapperClass;
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() {
        throwIfUnset(reducerClass, "reducerClass");
        return reducerClass;
    }

    public void setReducerClass(Class<? extends Reducer<?, ?, ?, ?>> reducerClass) {
        this.reducerClass = reducerClass;
    }

    public File getInputFile() {
        throwIfUnset(inputFile, "inputFile");
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputDir() {
        throwIfUnset(outputDir, "outputDir");
        return outputDir;
    }

    public void setOutputDir(File outputDir) {
        this.outputDir = outputDir;
    }

    public int getNumReducers() {
        throwIfUnset(numReducers, "numReducers");
        return numReducers;
    }

    public void setNumReducers(int numReducers) {
        this.numReducers = numReducers;
    }

    public int getNumMappers() {
        throwIfUnset(numMappers, "numMappers");
        return numMappers;
    }

    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(File tmpDir) {
        throwIfUnset(tmpDir, "tmpDir");
        this.tmpDir = tmpDir;
    }

    private void throwIfUnset(Object obj, String name) {
        if (obj instanceof Integer) {
            if ((int) obj == 0) {
                throw new IllegalStateException(name + " is not set");
            }
        } else {
            if (obj == null) {
                throw new IllegalStateException(name + " is not set");
            }
        }
    }
}
