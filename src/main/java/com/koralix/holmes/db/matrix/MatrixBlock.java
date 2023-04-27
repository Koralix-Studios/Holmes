package com.koralix.holmes.db.matrix;

import com.koralix.holmes.io.Serializable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MatrixBlock implements Serializable {

    public static final int DATA_TYPE = 1;
    private final List<Double> data;
    private final List<Integer> indices;
    private final List<Integer> indptr;

    public MatrixBlock(double[] data, int[] indices, int[] indptr) {
        this.data = Arrays.stream(data).boxed().collect(Collectors.toList());
        this.indices = Arrays.stream(indices).boxed().collect(Collectors.toList());
        this.indptr = Arrays.stream(indptr).boxed().collect(Collectors.toList());
    }

    public static MatrixBlock deserialize(ByteBuffer buffer) {
        int columns = buffer.getInt();
        int dataLength = buffer.getInt();
        double[] data = new double[dataLength];
        for (int i = 0; i < dataLength; i++) {
            data[i] = buffer.getDouble();
        }
        int[] indices = new int[dataLength];
        for (int i = 0; i < dataLength; i++) {
            indices[i] = buffer.getInt();
        }
        int[] indptr = new int[columns + 1];
        for (int i = 0; i <= columns; i++) {
            indptr[i] = buffer.getInt();
        }
        return new MatrixBlock(data, indices, indptr);
    }

    @Override
    public int requiredBytes() {
        return 4 * Integer.BYTES +
                data.size() * Double.BYTES +
                indices.size() * Integer.BYTES +
                indptr.size() * Integer.BYTES;
    }

    @Override
    public void flush(ByteBuffer buffer) {
        buffer.putInt(DATA_TYPE);
        buffer.putInt(indptr.size() - 1);
        buffer.putInt(data.size());
        for (double d : data) {
            buffer.putDouble(d);
        }
        for (int i : indices) {
            buffer.putInt(i);
        }
        for (int i : indptr) {
            buffer.putInt(i);
        }
    }

    public double get(long row, long column) {
        // If the indptr list is not long enough to cover the given column
        if (column >= indptr.size() - 1) {
            // Return 0 since the element at the given row and column must be 0
            return 0;
        }

        // Find the index of the first non-zero element in the given column
        int colStart = indptr.get((int) column);
        // Find the index of the last non-zero element in the given column
        int colEnd = indptr.get((int) column + 1);

        // Iterate over the non-zero elements in the given column
        for (int i = colStart; i < colEnd; i++) {
            // If a non-zero element is found at the given row
            if (indices.get(i) == row) {
                // Return its value
                return data.get(i);
            }
        }

        // If no non-zero element is found at the given row
        // Return 0 since the element at the given row and column must be 0
        return 0;
    }

    public void set(long row, long column, double value) {
        // If the indptr list is not long enough to cover the given column
        while (indptr.size() <= column + 1) {
            // Add a new element to the indptr list with the same value as the last element
            indptr.add(indptr.get(indptr.size() - 1));
        }

        // Find the index of the first non-zero element in the given column
        int colStart = indptr.get((int) column);
        // Find the index of the last non-zero element in the given column
        int colEnd = indptr.get((int) column + 1);

        // Iterate over the non-zero elements in the given column
        for (int i = colStart; i < colEnd; i++) {
            // If a non-zero element is found at the given row
            if (indices.get(i) == row) {
                // Update its value
                data.set(i, value);
                return;
            }
        }

        // If no non-zero element is found at the given row
        // Add a new non-zero element at the end of the column
        data.add(colEnd, value);
        indices.add(colEnd, (int) row);

        // Increment the indptr values for all columns after the given column
        for (int i = (int) column + 1; i < indptr.size(); i++) {
            indptr.set(i, indptr.get(i) + 1);
        }
    }
}
