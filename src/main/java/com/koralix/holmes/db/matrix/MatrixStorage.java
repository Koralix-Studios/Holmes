package com.koralix.holmes.db.matrix;

import com.koralix.holmes.Holmes;
import com.koralix.holmes.io.BinaryFileChannel;

import java.io.IOException;
import java.nio.file.Path;

public class MatrixStorage {

    private final BinaryFileChannel channel;
    private final Matrix matrix;

    public MatrixStorage(String name) throws IOException {
        final Path filePath = Path.of(name + ".hms");
        boolean exists = filePath.toFile().exists();
        channel = new BinaryFileChannel(filePath);
        if (exists) {
            matrix = channel.read(Matrix::deserialize);
        } else {
            matrix = new Matrix(name, 0, 0);
            channel.write(matrix);
        }
        Holmes.LOGGER.info("Opened matrix storage");
    }

    public void shutdown() {
        channel.write(matrix);
        channel.close();
    }

    public Matrix getMatrix() {
        return matrix;
    }
}
