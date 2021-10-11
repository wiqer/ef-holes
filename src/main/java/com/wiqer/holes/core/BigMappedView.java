package com.wiqer.holes.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

public class BigMappedView extends MappedViewModel {
    public static final String suffix=".efd";


    //最新添加的位置游标，当前的最右侧数据结束位置
    public volatile long putIndex=0;
    //上一个数据存储的下标位置，当前的最右侧数据起始位置
    public volatile long  prevColumnIndex=0;
    //    //最新添加的位置游标，当前的最右侧数据结束位置
//    public volatile long putLongIndex=0;
//    //上一个数据存储的下标位置，当前的最右侧数据起始位置
//    public volatile long  prevLongColumnIndex=0;
    MapInfo mapInfo;

    public MapInfo getMapInfo() {
        return mapInfo;
    }

    public BigMappedView(String filename) throws IOException {
        this.fileName=filename;
        File file=new File(filename+suffix);
        if(!file .isDirectory())
        {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                parentFile.mkdirs(); // 创建文件夹

            }
        }
        randomAccessFile = new RandomAccessFile(filename+suffix, "rw");
        channel= randomAccessFile.getChannel();
        mapInfo= new MapInfo(randomAccessFile);
        putIndex=mapInfo.putIndex;
    }

    public BigMappedView(File file) throws FileNotFoundException {
        if  (!file .exists()  && !file .isDirectory())
        {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                parentFile.mkdirs(); // 创建文件夹

            }
        }
        randomAccessFile = new RandomAccessFile(file, "rw");
        channel= randomAccessFile.getChannel();
    }
    private BigMappedView(){

    }
    public BigMappedView(String filename,String name,String info ) throws IOException {
        File file=new File(filename+suffix);
        if(!file .isDirectory())
        {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                parentFile.mkdirs(); // 创建文件夹

            }
        }
        randomAccessFile = new RandomAccessFile(filename+suffix, "rw");
        channel= randomAccessFile.getChannel();
        fileName=filename;
        mapInfo= new MapInfo(randomAccessFile,name,info);
        putIndex=mapInfo.putIndex;
    }
    public int getIntPutIndex() {
        return (int)putIndex;

    }

    public int getIntPrevColumnIndex() {
        return (int)prevColumnIndex;
    }
    public  int put(int id, byte[] bytes){
        int index=0;
        int prevColIndex=0;
        synchronized (this){
            prevColIndex= this.getIntPrevColumnIndex() ;
            this.prevColumnIndex= index=this.getIntPutIndex();
            this.putIndex+= MapColumn.columnInfoLen+bytes.length;
        }

        try{
            return  doPut(id,bytes, index,prevColIndex);
        }catch (Exception e){
            e.printStackTrace();
        }
        return index;
    }
    public  int doPut(int id,byte[] bytes,int index,   int prevColIndex) throws IOException {
        return  new MapColumn(randomAccessFile,id,bytes, (byte) 0x0,index,prevColIndex,true).columnIndex;
    }
    public  int put(byte[] bytes){
        int index=0;
        synchronized (this){
            index=this.getIntPutIndex();
            this.putIndex+=3+bytes.length;
        }

        try{
            doPut(bytes, index);
        }catch (Exception e){
            e.printStackTrace();
        }
        return index;
    }
    public  void doPut(byte[] bytes,int index) throws Exception {

        //获取对应的通道
        FileChannel channel = randomAccessFile.getChannel();

        /**
         * 参数1: FileChannel.MapMode.READ_WRITE 使用的读写模式
         * 参数2： 0 ： 可以直接修改的起始位置
         * 参数3:  5: 是映射到内存的大小(不是索引位置) ,即将 1.txt 的多少个字节映射到内存
         * 可以直接修改的范围就是 0-5
         * 实际类型 DirectByteBuffer
         */
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, index, bytes.length+3);
        int writeIndex=0;
        for(int i=2;i>=0;i--){
            mappedByteBuffer.put(writeIndex++, (byte)( bytes.length>>>(i*8)));
        }
        for (byte aByte : bytes) {
            mappedByteBuffer.put(writeIndex++, aByte);
        }
        //randomAccessFile.close();
        System.out.println("修改成功~~");
    }
    public  byte[]  get(int index) {

        //获取对应的通道
        FileChannel channel = randomAccessFile.getChannel();
        try{
            /**
             * 参数1: FileChannel.MapMode.READ_WRITE 使用的读写模式
             * 参数2： 0 ： 可以直接修改的起始位置
             * 参数3:  5: 是映射到内存的大小(不是索引位置) ,即将 1.txt 的多少个字节映射到内存
             * 可以直接修改的范围就是 0-5
             * 实际类型 DirectByteBuffer
             */
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, index, 3);
            //数据长度
            int btl=0;
            //mappedByteBuffer的索引
            int readIndex=0;
            for(int i=2;i>=0;i--){
                btl+= (mappedByteBuffer.get(readIndex++)& 0xff)<<(i*8);
            }
            byte[] bytes =new byte[btl];
            MappedByteBuffer mappedByteBufferData = channel.map(FileChannel.MapMode.READ_ONLY, index+3, btl);
            for(int i=0;i<bytes.length;i++){
                bytes[i] =mappedByteBufferData.get(i);
            }
            System.out.println("修改成功~~");
            return bytes;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    static class  MapColumn extends MappedViewModel {
        public  static final  int columnInfoLen=12;
        public  static final int columnNextColLenIndex=4;//物理磁盘上下一行数据起始下标占位
        public  static final int columnLenIndex=3;//数据行长度
        public  static final int columnIdLenIndex=4;//数据id占位
        //public  static final int columnTransactionLenIndex=4;//数据事务id
        public  static final int columnInfoTypeLenIndex=1;//数据类型，json，二进制，gzip，zip，7z等，JDK deflate，LZ4压缩算法，Snappy
        public    int columnFormatLen;
        private int id=-1;
        public    int  nextColumnIndex=-1;
        private int  columnIndex=-1;
        private byte type=-1;

        public int getNextColumnIndex() {
            return nextColumnIndex;
        }
        public int getId() {
            return id;
        }
        // public  static final int columnInfoTypeLen=1;
        /*新建数据*/
        public  MapColumn(RandomAccessFile randomAccessFile,int id,byte[] bytes,byte type,int columnIndex,int prevColumnIndex,/*是否将当前数据追加到右侧*/boolean next) throws IOException {
            this.randomAccessFile=randomAccessFile;
            channel= randomAccessFile.getChannel();
            this.columnIndex=columnIndex;
            this.id=id;
            /*右追加*/
            if(next){
                super.putLen(prevColumnIndex,columnNextColLenIndex,columnIndex);
            }
            /*左追加*/
            else {
                this.nextColumnIndex=prevColumnIndex;
                super.putLen(columnIndex,columnNextColLenIndex,prevColumnIndex);
            }
            int index=columnIndex+columnNextColLenIndex;
            columnFormatLen=bytes.length;
            index= super.putLen(index,columnLenIndex,columnFormatLen);
            index= super.putLen(index,columnIdLenIndex,id);
            index= super.putLen(index,columnInfoTypeLenIndex,type);
            super.putBytes(index,bytes);
        }
        /*恢复数据*/
        public  MapColumn(RandomAccessFile randomAccessFile,int columnIndex) throws IOException {
            this.randomAccessFile=randomAccessFile;
            channel= randomAccessFile.getChannel();
            this.columnIndex=columnIndex;
            nextColumnIndex =super.getLen(columnIndex,columnNextColLenIndex);
            int index=columnIndex+columnNextColLenIndex;
            columnFormatLen=super.getLen(index,columnLenIndex);
            index+=columnLenIndex;
            id=super.getLen(index,columnIdLenIndex);
            index+=columnIdLenIndex;
            type=(byte)super.getLen(index,columnInfoTypeLenIndex);
        }
        public static byte[] getColDataBinary(RandomAccessFile randomAccessFile,int columnIndex){
            FileChannel channel = randomAccessFile.getChannel();
            try{
                /**
                 * 参数1: FileChannel.MapMode.READ_WRITE 使用的读写模式
                 * 参数2： 0 ： 可以直接修改的起始位置
                 * 参数3:  5: 是映射到内存的大小(不是索引位置) ,即将 1.txt 的多少个字节映射到内存
                 * 可以直接修改的范围就是 0-5
                 * 实际类型 DirectByteBuffer
                 */
                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, columnIndex+columnNextColLenIndex, 3);
                //数据长度
                int btl=0;
                //mappedByteBuffer的索引
                int readIndex=0;
                for(int i=2;i>=0;i--){
                    btl+= (mappedByteBuffer.get(readIndex++)& 0xff)<<(i*8);
                }
                if(btl<=0) throw new EmptyDataColException();
                byte[] bytes =new byte[btl];
                MappedByteBuffer mappedByteBufferData = channel.map(FileChannel.MapMode.READ_ONLY, columnIndex+columnInfoLen, btl);
                for(int i=0;i<bytes.length;i++){
                    bytes[i] =mappedByteBufferData.get(i);
                }

                return bytes;
            }catch (Exception e){
                e.printStackTrace();
            }

            return null;
        }
    }
    /*表信息*/
    class  MapInfo extends MappedViewModel {
        private ReentrantLock lock = new ReentrantLock();
        private     int formatLen;
        public   final int minColumnLenIndex=8;//数据id占位
        public   final int mapFormatLenIndex=2;//总长度占位,没啥实际用处
        public   final int nameLenIndex=1;//数据行长度
        public   final int infoLenIndex=2;//数据行长度
        public String name;
        public String info;
        //当前的最小，左索引
        public int index=0;
        public long putIndex;
        MapInfo(RandomAccessFile randomAccessFile, String name,String info) throws IOException {
            this.randomAccessFile=randomAccessFile;
            channel= randomAccessFile.getChannel();
            this.name=name;
            this.info=info;
            byte[] nameBytes=name.getBytes(StandardCharsets.UTF_8);
            byte[] infoBytes=info.getBytes(StandardCharsets.UTF_8);
            formatLen=infoBytes.length+nameBytes.length+mapFormatLenIndex+minColumnLenIndex+nameLenIndex+infoLenIndex;
            index= super.putLen(index,minColumnLenIndex,formatLen);
            index= super.putLen(index,mapFormatLenIndex,formatLen);
            index= super.putLen(index,nameLenIndex,name.length());
            index= super.putLen(index,infoLenIndex,info.length());
            index= super.putBytes(index,nameBytes);
            index= super.putBytes(index,infoBytes);

        }
        /*恢复数据*/
        MapInfo(RandomAccessFile randomAccessFile) throws IOException {

            int nameLen=0;//数据行长度
            int infoLen=0;//数据行长度
            this.randomAccessFile=randomAccessFile;
            channel= randomAccessFile.getChannel();
            putIndex=super.getLenLong(index,minColumnLenIndex);
            index+=minColumnLenIndex;
            formatLen=super.getLen(index,mapFormatLenIndex);
            index+=mapFormatLenIndex;
            nameLen=super.getLen(index,nameLenIndex);
            index+=nameLenIndex;
            infoLen=super.getLen(index,infoLenIndex);
            index+=infoLenIndex;
            byte[] nameBytes=super.getBytes(index,nameLen);
            index+=nameLen;
            byte[] infoBytes=super.getBytes(index,infoLen);
            this.name= new String(nameBytes, StandardCharsets.UTF_8);
            this.info=new String(infoBytes, StandardCharsets.UTF_8);
        }
        /*移动初始索引*/
        int shiftStartIndex(int minIndex) throws IOException {
            //this.index=minIndex;
            boolean locked=lock.tryLock();
            if(locked){
                try {
                    putLen(mapFormatLenIndex,minColumnLenIndex,minIndex);
                    return minIndex;
                }finally {
                    lock.unlock();
                }
            }else {
                return -1;
            }

        }
        int getLeftIndex(int putLen) throws IOException {
            boolean locked=lock.tryLock();
            if(locked&&formatLen-this.index<=putLen){
                try {
                    return this.index-=putLen;
                }finally {
                    // this.index=shiftIndex(this.index-putLen);
                    lock.unlock();
                }

            }else {
                return  -1;
            }
        }
        // public  static final int columnInfoTypeLen=1;
    }
}
