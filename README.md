# simple-mapreduce

This is a simple implementation of the MapReduce programming model in Java. The implementation is based on the paper *"MapReduce: Simplified Data Processing on Large Clusters"* by Jeffrey Dean and Sanjay Ghemawat.

## :rocket: Quick Start

### Requirements:
    - Java 17+
    - Maven

Download the jar file from the [releases page](https://github.com/llipengda/simple-mapreduce/releases) and add ite to your project.

### Writing a MapReduce job

Write a `Mapper` and a `Reducer` class.
For example, counting the number of words in a text file:

```java
public class WordCountMapper extends Mapper<Long, String, String, Integer> {
    @Override
    public void map(Long key, String value, Context<String, Integer> context) {
        String[] words = value.split("\\s+");
        for (String word : words) {
            context.write(word, 1);
        }
    }
}
```
```java
public class WordCountReducer extends Reducer<String, Integer, String, Integer> {
    @Override
    public void reduce(String key, Iterable<Integer> values, Context<String, Integer> context) {
        int sum = 0;
        for (int value : values) {
            sum += value;
        }
        context.write(key, sum);
    }
}
```

Create a class containing the main method and add configuration:

- MapperClass
- ReducerClass
- InputFile
- OutputDir
- Workers

```java
public class Main {
    public static void main(String[] args) {
        var config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);

        config.addWorker("localhost");
        config.addWorker("localhost");
        config.addWorker("localhost");
        config.addWorker("localhost");
        config.addWorker("localhost");

        config.setNumReducers(2);
        config.setSplitChunkSize(100 * 1000);

        config.setInputFile(new File(args[0]));
        config.setOutputDir(new File(args[1]));

        Runner runner = new LocalRunner();

        runner.run();
        runner.waitForCompletion();
    }
}
```

### Running

#### Locally

Just run the main method of the class you created.

#### Distributed

0. Make sure that the distributed machines can access each other via SSH without password.

1. Modify the `Main` class to use the `DistributedRunner` and add some necessary configurations:

- MainClass
- MasterPort
- JarPath

```java
public class Main {
    public static void main(String[] args) {
        var config = Config.getInstance();

        config.setMapperClass(WordCountMapper.class);
        config.setReducerClass(WordCountReducer.class);
        config.setMainClass(WordCountDistributed.class);
        config.setMasterPort(10000);

        config.addWorker("node2", 10000);
        config.addWorker("node3", 10000);
        config.setJarPath("word-count.jar");

        config.setNumReducers(1);
        config.setSplitChunkSize(100 * 1000);

        config.setInputFile(new File("input.txt"));
        config.setOutputDir(new File("out"));

        Runner runner = new DistributedRunner(args);

        runner.run();
        runner.waitForCompletion();
    }
}
```

Note that the `DistributedRunner` constructor takes an array of strings as an argument. Pass the `args` array to the constructor. Do not modify the `args` array.

When running distributed, you need to explicit set the ports for the workers and master. The workers will connect to the master using this port.

2. Package your project into a fat jar file with all dependencies.

3. Run the jar file on the master machine.

```bash
java -cp word-count.jar Main
```

You don't need to run the jar file on the worker machines. The master will take care of that. And you don't need to pass any arguments to the jar file.

#### Distributed with Docker

You can also stimulate a distributed environment using Docker.

1. Run a docker image, e.g., `ubuntu:latest`.

```bash
docker run -it --name node1 ubuntu:latest
```

2. Install libraries and dependencies on the docker image.

```bash
docker exec -it node1 bash
```
```bash
apt-get update
apt-get install -y openjdk-17-jdk maven
apt-get install -y openssh-server
```

3. Modify sshd_config file.

```bash
vi /etc/ssh/sshd_config
```

Change `PermitRootLogin prohibit-password` to `PermitRootLogin yes`.

4. Restart the ssh service.

```bash
service ssh restart
```

5. Set a password for the root user.

```bash
passwd
```

6. Exit the docker container. Then commit the docker container to an image.

```bash
exit
```
```bash
docker commit node1 ubuntu-ssh
```

8. Run the docker image for more nodes.

```bash
docker run -it --name node2 ubuntu-ssh
docker run -it --name node3 ubuntu-ssh
```

9. Create a network for the docker containers. Then connect the containers to the network.

```bash
docker network create my-network
docker network connect my-network node1
docker network connect my-network node2
docker network connect my-network node3
```

10. For **each** node, generate an SSH key pair. Then use `ssh-copy-id` to copy the public key to the other nodes.

```bash
docker exec -it node1 bash
```
```bash
ssh-keygen -t rsa
ssh-copy-id node2
ssh-copy-id node3
```

11. Copy the jar file to the master node.

```bash
docker cp word-count.jar node1:/word-count.jar
```

12. Run the jar file on the master node.

```bash
docker exec -it node1 bash
```
```bash
java -cp word-count.jar Main
```

## :hammer: Build

This project uses Maven and JDK 17. To build the project, run the following command:

```bash
mvn clean compile package
```

`compile` is for the grpc plugin to generate the necessary classes.

Some tests may fail. You can build the project without running the tests:

```bash
mvn clean compile package -DskipTests
```

## :wrench: Configuration

There exists a `Config` singleton class that holds the configurations for the MapReduce job. You can set the configurations using the `set` methods.

The table below shows the available configurations:

`*` denotes that the configuration is required for local and distributed running.

`^` denotes that the configuration is required for distributed running.

| Config | Description | Default Value |
| --- | --- | --- |
| *`mapperClass` | The class of the mapper. | unset |
| *`reducerClass` | The class of the reducer. | unset |
| ^`mainClass` | The class containing the main method. | unset |
| *`inputFile` | The input file. | unset |
| *`outputDir` | The output directory. | unset |
| `tmpDir` | The temporary directory. | `tmp` |
| *`workers` | The list of workers. Using `addWorker` to add. | `[]` |
| `numMappers` | The number of mappers. Only works when `splitMethod` is set to `BY_NUM_MAPPERS` | unset |
| `numReducers` | The number of reducers. | `1` |
| ^`masterPort` | The port of the master. 0 for random. | `0` |
| `splitChunkSize` | The size of the split chunk. | `1024 * 1024 * 64 (64MB)` |
| ^`jarPath` | The path to the jar file. | unset |
| `distributedMasterWaitTime` | The time master wait for the workers to start. Formats in ms. Only works when running distributed | `2000` |
| `splitMethod` | The method to split the input file. `BY_CHUNK_SIZE` or `BY_NUM_MAPPERS`. | `BY_CHUNK_SIZE` |
| `usingLocalFileSystemForLocalhost` | Whether to use the local file system for localhost. | `true` |

## :page_facing_up: License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
