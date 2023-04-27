package com.koralix.holmes;

import com.koralix.holmes.db.matrix.Matrix;
import com.koralix.holmes.db.matrix.MatrixStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Holmes {

    public static final Logger LOGGER = LoggerFactory.getLogger(Holmes.class);
    private final MatrixStorage storage;

    public static void main(String[] args) {
        new Holmes();
    }

    public Holmes() {
        LOGGER.info("Starting Holmes");

        try {
            storage = new MatrixStorage("tf-idf");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Stopping Holmes");

        storage.shutdown();
    }

}
