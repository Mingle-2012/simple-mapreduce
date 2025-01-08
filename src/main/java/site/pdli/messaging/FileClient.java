package site.pdli.messaging;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileClient {
    private final FileServiceGrpc.FileServiceBlockingStub blockingStub;

    private final Logger log = LoggerFactory.getLogger(FileClient.class);


    public FileClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();
        this.blockingStub = FileServiceGrpc.newBlockingStub(channel);
    }

    public ByteString readFile(String filePath) {
        log.info("Reading file {}", filePath);
        File.ReadFileRequest request = File.ReadFileRequest.newBuilder()
                .setFilePath(filePath)
                .build();
        File.ReadFileResponse response = blockingStub.readFile(request);
        return response.getContent();
    }

    public void writeFile(String filePath, byte[] content) {
        File.WriteFileRequest request = File.WriteFileRequest.newBuilder()
                .setFilePath(filePath)
                .setContent(ByteString.copyFrom(content))
                .build();
        var res = blockingStub.writeFile(request);

        if (res.getSuccess()) {
            log.info("Writing file {} successfully", filePath);
        } else {
            log.error("Failed to write file {}", filePath);
        }
    }
}
