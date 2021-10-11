package com.wiqer.holes.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public abstract class MappedViewModel {
    public String fileName ;
    public RandomAccessFile randomAccessFile ;
    public FileChannel channel ;
    /*获取长度int*/
    public int getLen(int index,int indexLen) throws IOException {
        //FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, index, indexLen);
        //数据长度
        int btl=0;
        //mappedByteBuffer的索引
        int readIndex=0;
        for(int i=indexLen-1;i>=0;i--){
            btl+= (mappedByteBuffer.get(readIndex++)& 0xff)<<(i*8);
        }
        return btl;
    }
    /*写入长度int*/
    public int putLen(int index,int indexLen,int val) throws IOException {
       // FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, index, indexLen);
        int writeIndex=0;
        for(int i=indexLen-1;i>=0;i--){
            mappedByteBuffer.put(writeIndex++, (byte)( val>>(i*8)));
        }
        return index+indexLen;
    }
    /*获取长度int*/
    public long getLenLong(int index,int indexLen) throws IOException {
        //FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, index, indexLen);
        //数据长度
        long btl=0;
        //mappedByteBuffer的索引
        int readIndex=0;
        for(int i=indexLen-1;i>=0;i--){
            btl+= (mappedByteBuffer.get(readIndex++)& 0xff)<<(i*8);
        }
        return btl;
    }
    /*写入长度int*/
    public int putLenLong(int index,int indexLen,long val) throws IOException {
        // FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, index, indexLen);
        int writeIndex=0;
        for(int i=indexLen-1;i>=0;i--){
            mappedByteBuffer.put(writeIndex++, (byte)( val>>(i*8)));
        }
        return index+indexLen;
    }
    /*写入长度int*/
    public int putBytes(int index,byte[] bytes) throws IOException {
        //FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, index, bytes.length);
        for(int i=0;i<bytes.length;i++){
            mappedByteBuffer.put(i, bytes[i]);
        }
        return index+bytes.length;
    }
    public byte[] getBytes(int index,int len) throws IOException {
        //获取对应的通道
        //FileChannel channel = randomAccessFile.getChannel();
        byte[] bytes =new byte[len];
        MappedByteBuffer mappedByteBufferData = channel.map(FileChannel.MapMode.READ_ONLY, index, len);
        for(int i=0;i<bytes.length;i++){
            bytes[i] =mappedByteBufferData.get(i);
        }
        return bytes;
    }
}
