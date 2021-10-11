package com.wiqer.holes.util.io;


import com.dyuproject.protostuff.*;

import java.io.IOException;

/*
com.dyuproject.protostuff.* 的反序列化池，提升速度80%
 */
public class ByteArrayInputPool  implements Input {
    private final byte[] buffer;
    private int offset;
    private int limit;
    private int lastTag;
    private int capacity;
    public final boolean decodeNestedMessageAsGroup;
    public ByteArrayInputPool() {
        this(new byte[512]);
    }
    public ByteArrayInputPool(byte[] buffer) {
        this(buffer, 0, buffer.length, true);
    }
    public ByteArrayInputPool(byte[] buffer, boolean decodeNestedMessageAsGroup) {
        this(buffer, 0, buffer.length, decodeNestedMessageAsGroup);
    }

    public ByteArrayInputPool(byte[] buffer, int offset, int len, boolean decodeNestedMessageAsGroup) {

        this.lastTag = 0;
        this.buffer = buffer;
        this.offset = offset;
        this.limit = offset + len;
        this.capacity=buffer.length;
        this.decodeNestedMessageAsGroup = decodeNestedMessageAsGroup;
    }

    public  byte get(int index) {
        return buffer[ this.offset+index];
    }


    public  void put(int index, byte b) {
         buffer[ this.offset+index]=b;
    }

    public ByteArrayInputPool reset(int offset, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("length cannot be negative.");
        } else {
            this.offset = offset;
            this.limit = offset + len;
            return this;
        }
    }



    public int currentOffset() {
        return this.offset;
    }

    public int currentLimit() {
        return this.limit;
    }

    public int getLastTag() {
        return this.lastTag;
    }

    public int readTag() throws IOException {
        if (this.offset == this.limit) {
            this.lastTag = 0;
            return 0;
        } else {
            int tag = this.readRawVarint32();
            if (tag >>> 3 == 0) {
                throw new IOException();
            } else {
                this.lastTag = tag;
                return tag;
            }
        }
    }

    public void checkLastTagWas(int value) throws IOException {
        if (this.lastTag != value) {
            throw  new IOException();
        }
    }

    public boolean skipField(int tag) throws IOException {
        switch(WireFormat.getTagWireType(tag)) {
            case 0:
                this.readInt32();
                return true;
            case 1:
                this.readRawLittleEndian64();
                return true;
            case 2:
                int size = this.readRawVarint32();
                if (size < 0) {
                    throw  new IOException();
                }

                this.offset += size;
                return true;
            case 3:
                this.skipMessage();
                this.checkLastTagWas(WireFormat.makeTag(WireFormat.getTagFieldNumber(tag), 4));
                return true;
            case 4:
                return false;
            case 5:
                this.readRawLittleEndian32();
                return true;
            default:
                throw  new IOException();
        }
    }

    public void skipMessage() throws IOException {
        int tag;
        do {
            tag = this.readTag();
        } while(tag != 0 && this.skipField(tag));

    }

    public <T> void handleUnknownField(int fieldNumber, Schema<T> schema) throws IOException {
        this.skipField(this.lastTag);
    }

    public <T> int readFieldNumber(Schema<T> schema) throws IOException {
        if (this.offset == this.limit) {
            this.lastTag = 0;
            return 0;
        } else {
            int tag = this.readRawVarint32();
            int fieldNumber = tag >>> 3;
            if (fieldNumber == 0) {
                if (this.decodeNestedMessageAsGroup && 7 == (tag & 7)) {
                    this.lastTag = 0;
                    return 0;
                } else {
                    throw  new IOException();
                }
            } else if (this.decodeNestedMessageAsGroup && 4 == (tag & 7)) {
                this.lastTag = 0;
                return 0;
            } else {
                this.lastTag = tag;
                return fieldNumber;
            }
        }
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(this.readRawLittleEndian64());
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(this.readRawLittleEndian32());
    }

    public long readUInt64() throws IOException {
        return this.readRawVarint64();
    }

    public long readInt64() throws IOException {
        return this.readRawVarint64();
    }

    public int readInt32() throws IOException {
        return this.readRawVarint32();
    }

    public long readFixed64() throws IOException {
        return this.readRawLittleEndian64();
    }

    public int readFixed32() throws IOException {
        return this.readRawLittleEndian32();
    }

    public boolean readBool() throws IOException {
        return this.buffer[this.offset++] != 0;
    }

    public int readUInt32() throws IOException {
        return this.readRawVarint32();
    }

    public int readEnum() throws IOException {
        return this.readRawVarint32();
    }

    public int readSFixed32() throws IOException {
        return this.readRawLittleEndian32();
    }

    public long readSFixed64() throws IOException {
        return this.readRawLittleEndian64();
    }

    public int readSInt32() throws IOException {
        int n = this.readRawVarint32();
        return n >>> 1 ^ -(n & 1);
    }

    public long readSInt64() throws IOException {
        long n = this.readRawVarint64();
        return n >>> 1 ^ -(n & 1L);
    }

    public String readString() throws IOException {
        int length = this.readRawVarint32();
        if (length < 0) {
            throw  new IOException();
        } else if (this.offset + length > this.limit) {
            throw new IOException();
        } else {
            int offset = this.offset;
            this.offset += length;
            return StringSerializer.STRING.deser(this.buffer, offset, length);
        }
    }

    public ByteString readBytes() throws IOException {
        return  ByteString.copyFrom(this.readByteArray());
    }

    public byte[] readByteArray() throws IOException {
        int length = this.readRawVarint32();
        if (length < 0) {
            throw new IOException();
        } else if (this.offset + length > this.limit) {
            throw new IOException();
        } else {
            byte[] copy = new byte[length];
            System.arraycopy(this.buffer, this.offset, copy, 0, length);
            this.offset += length;
            return copy;
        }
    }

    public <T> T mergeObject(T value, Schema<T> schema) throws IOException {
        if (this.decodeNestedMessageAsGroup) {
            return this.mergeObjectEncodedAsGroup(value, schema);
        } else {
            int length = this.readRawVarint32();
            if (length < 0) {
                throw  new IOException();
            } else {
                int oldLimit = this.limit;
                this.limit = this.offset + length;
                if (value == null) {
                    value = schema.newMessage();
                }

                schema.mergeFrom(this, value);
                if (!schema.isInitialized(value)) {
                    throw new UninitializedMessageException(value, schema);
                } else {
                    this.checkLastTagWas(0);
                    this.limit = oldLimit;
                    return value;
                }
            }
        }
    }

    private <T> T mergeObjectEncodedAsGroup(T value, Schema<T> schema) throws IOException {
        if (value == null) {
            value = schema.newMessage();
        }

        schema.mergeFrom(this, value);
        if (!schema.isInitialized(value)) {
            throw new UninitializedMessageException(value, schema);
        } else {
            this.checkLastTagWas(0);
            return value;
        }
    }

    public int readRawVarint32() throws IOException {
        byte tmp = this.buffer[this.offset++];
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = this.buffer[this.offset++]) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = this.buffer[this.offset++]) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = this.buffer[this.offset++]) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = this.buffer[this.offset++]) << 28;
                        if (tmp < 0) {
                            for(int i = 0; i < 5; ++i) {
                                if (this.buffer[this.offset++] >= 0) {
                                    return result;
                                }
                            }

                            throw  new IOException();
                        }
                    }
                }
            }

            return result;
        }
    }

    public long readRawVarint64() throws IOException {
        byte[] buffer = this.buffer;
        int offset = this.offset;
        int shift = 0;

        for(long result = 0L; shift < 64; shift += 7) {
            byte b = buffer[offset++];
            result |= (long)(b & 127) << shift;
            if ((b & 128) == 0) {
                this.offset = offset;
                return result;
            }
        }

        throw  new IOException();
    }

    public int readRawLittleEndian32() throws IOException {
        byte[] buffer = this.buffer;
        int offset = this.offset;
        byte b1 = buffer[offset++];
        byte b2 = buffer[offset++];
        byte b3 = buffer[offset++];
        byte b4 = buffer[offset++];
        this.offset = offset;
        return b1 & 255 | (b2 & 255) << 8 | (b3 & 255) << 16 | (b4 & 255) << 24;
    }

    public long readRawLittleEndian64() throws IOException {
        byte[] buffer = this.buffer;
        int offset = this.offset;
        byte b1 = buffer[offset++];
        byte b2 = buffer[offset++];
        byte b3 = buffer[offset++];
        byte b4 = buffer[offset++];
        byte b5 = buffer[offset++];
        byte b6 = buffer[offset++];
        byte b7 = buffer[offset++];
        byte b8 = buffer[offset++];
        this.offset = offset;
        return (long)b1 & 255L | ((long)b2 & 255L) << 8 | ((long)b3 & 255L) << 16 | ((long)b4 & 255L) << 24 | ((long)b5 & 255L) << 32 | ((long)b6 & 255L) << 40 | ((long)b7 & 255L) << 48 | ((long)b8 & 255L) << 56;
    }

    public void transferByteRangeTo(Output output, boolean utf8String, int fieldNumber, boolean repeated) throws IOException {
        int length = this.readRawVarint32();
        if (length < 0) {
            throw  new IOException();
        } else {
            output.writeByteRange(utf8String, fieldNumber, this.buffer, this.offset, length, repeated);
            this.offset += length;
        }
    }


}
