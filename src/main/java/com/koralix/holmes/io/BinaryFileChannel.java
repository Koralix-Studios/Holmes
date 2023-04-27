package com.koralix.holmes.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class BinaryFileChannel implements AutoCloseable {

    private final FileChannel channel;

    public BinaryFileChannel(Path path) throws IOException {
        if (path.getParent() != null && !path.getParent().toFile().exists() && !path.getParent().toFile().mkdirs()) {
            throw new IOException("Failed to create parent directories");
        }
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
    }

    public <T extends Serializable> T read(Function<ByteBuffer, T> deserializer) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        try {
            channel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        buffer = ByteBuffer.allocate(size);
        try {
            channel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.flip();
        return deserializer.apply(buffer);
    }

    public void write(Serializable serializable) {
        ByteBuffer buffer = ByteBuffer.allocate(serializable.requiredBytes() + Integer.BYTES);
        buffer.putInt(serializable.requiredBytes());
        serializable.flush(buffer);
        buffer.flip();
        try {
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
