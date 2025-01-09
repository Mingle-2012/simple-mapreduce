package site.pdli.mapreduce.messaging;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileService extends FileServiceGrpc.FileServiceImplBase {
    @Override
    public void readFile(File.ReadFileRequest request, StreamObserver<File.ReadFileResponse> responseObserver) {
        String filePath = request.getFilePath();
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            File.ReadFileResponse response = File.ReadFileResponse.newBuilder()
                    .setContent(ByteString.copyFrom(content))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to read file").withCause(e).asException());
        }
    }

    @Override
    public void writeFile(File.WriteFileRequest request, StreamObserver<File.WriteFileResponse> responseObserver) {
        String filePath = request.getFilePath();
        ByteString content = request.getContent();
        try {
            Files.write(Paths.get(filePath), content.toByteArray());
            File.WriteFileResponse response = File.WriteFileResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to write file").withCause(e).asException());
        }
    }
}
