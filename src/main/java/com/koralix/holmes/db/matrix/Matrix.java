package com.koralix.holmes.db.matrix;

import com.koralix.holmes.Holmes;
import com.koralix.holmes.cache.LRU;
import com.koralix.holmes.io.BinaryFileChannel;
import com.koralix.holmes.io.Serializable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class Matrix implements Serializable {

    public static final int DATA_TYPE = 0;
    private static final int BLOCK_SIZE = 1024;
    private final String name;
    private final LRU<Long, MatrixBlock> cache;
    private long rows;
    private long columns;

    public Matrix(String name, long rows, long columns) {
        this.name = name;
        this.rows = rows;
        this.columns = columns;
        this.cache = new LRU<>(100, (id, matrixBlock) -> {
            try (BinaryFileChannel channel = new BinaryFileChannel(Path.of("blocks/" + name + "-" + id + ".hmb"))) {
                channel.write(matrixBlock);
            } catch (IOException e) {
                Holmes.LOGGER.error("Failed to write matrix block", e);
            }
        });
    }

    public static Matrix deserialize(ByteBuffer buffer) {
        StringBuilder name = new StringBuilder();
        char c;
        while ((c = buffer.getChar()) != '\0') {
            name.append(c);
        }
        long rows = buffer.getLong();
        long columns = buffer.getLong();
        return new Matrix(name.toString(), rows, columns);
    }

    private MatrixBlock getBlock(long row, long column) {
        if (row >= rows) {
            rows = row + 1;
            cache.clear();
        }
        if (column >= columns) {
            columns = column + 1;
        }
        long blockId = (column / BLOCK_SIZE) * (int)Math.ceil((double) rows / BLOCK_SIZE) + (row / BLOCK_SIZE);
        return cache.computeIfAbsent(blockId, id -> {
            Path filePath = Path.of("blocks/" + name + "-" + id + ".hmb");
            if (filePath.toFile().exists()) {
                try (BinaryFileChannel channel = new BinaryFileChannel(filePath)) {
                    return channel.read(MatrixBlock::deserialize);
                } catch (IOException e) {
                    Holmes.LOGGER.error("Failed to read matrix block", e);
                }
            }

            return new MatrixBlock(
                    new double[0],
                    new int[0],
                    new int[] { 0 }
            );
        });
    }

    public double get(long row, long column) {
        return getBlock(row, column).get(row % BLOCK_SIZE, column % BLOCK_SIZE);
    }

    public void set(long row, long column, double value) {
        getBlock(row, column).set(row % BLOCK_SIZE, column % BLOCK_SIZE, value);
    }

    @Override
    public int requiredBytes() {
        return Integer.BYTES + (name.length() + 1) * Character.BYTES + 2 * Long.BYTES;
    }

    @Override
    public void flush(ByteBuffer buffer) {
        buffer.putInt(DATA_TYPE);
        for (char c : name.toCharArray()) {
            buffer.putChar(c);
        }
        buffer.putChar('\0');
        buffer.putLong(rows);
        buffer.putLong(columns);

        cache.save();
    }
}
