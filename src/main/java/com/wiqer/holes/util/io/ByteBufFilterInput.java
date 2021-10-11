package com.wiqer.holes.util.io;

import com.dyuproject.protostuff.ByteString;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

import java.io.IOException;

public class ByteBufFilterInput<F extends Input> implements Input {
    protected final F input;

    public ByteBufFilterInput(F input) {
        this.input = input;
    }

    public <T> void handleUnknownField(int fieldNumber, Schema<T> schema) throws IOException {
        this.input.handleUnknownField(fieldNumber, schema);
    }

    public <T> int readFieldNumber(Schema<T> schema) throws IOException {
        return this.input.readFieldNumber(schema);
    }

    public boolean readBool() throws IOException {
        return this.input.readBool();
    }

    public byte[] readByteArray() throws IOException {
        return this.input.readByteArray();
    }

    public ByteString readBytes() throws IOException {
        return this.input.readBytes();
    }

    public double readDouble() throws IOException {
        return this.input.readDouble();
    }

    public int readEnum() throws IOException {
        return this.input.readEnum();
    }

    public int readFixed32() throws IOException {
        return this.input.readFixed32();
    }

    public long readFixed64() throws IOException {
        return this.input.readFixed64();
    }

    public float readFloat() throws IOException {
        return this.input.readFloat();
    }

    public int readInt32() throws IOException {
        return this.input.readInt32();
    }

    public long readInt64() throws IOException {
        return this.input.readInt64();
    }

    public int readSFixed32() throws IOException {
        return this.input.readSFixed32();
    }

    public long readSFixed64() throws IOException {
        return this.input.readSFixed64();
    }

    public int readSInt32() throws IOException {
        return this.input.readSInt32();
    }

    public long readSInt64() throws IOException {
        return this.input.readSInt64();
    }

    public String readString() throws IOException {
        return this.input.readString();
    }

    public int readUInt32() throws IOException {
        return this.input.readUInt32();
    }

    public long readUInt64() throws IOException {
        return this.input.readUInt64();
    }

    public <T> T mergeObject(T value, Schema<T> schema) throws IOException {
        return this.input.mergeObject(value, schema);
    }

    public void transferByteRangeTo(Output output, boolean utf8String, int fieldNumber, boolean repeated) throws IOException {
        this.input.transferByteRangeTo(output, utf8String, fieldNumber, repeated);
    }
}
