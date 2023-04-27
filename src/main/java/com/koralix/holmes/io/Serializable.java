package com.koralix.holmes.io;

import java.nio.ByteBuffer;

public interface Serializable {

    int requiredBytes();

    void flush(ByteBuffer buffer);

}
