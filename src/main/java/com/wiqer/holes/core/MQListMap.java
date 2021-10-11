package com.wiqer.holes.core;



import com.wiqer.holes.util.EasyFastFormat;
import com.wiqer.holes.util.SerializationUtils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;


//默认单线程使用
public class MQListMap<V> extends EasyFastSkipListMap<Integer,V> {

    //volatile  Integer nextId=new Integer(0);
    //Call[]分段CAS+@sun.misc.Contended线程可见性，遍历获取缓存行，并非一条指令读，无法保证强一致性
    LongAdder adder=new LongAdder();
    //持久化
    MappedView mappedView ;
    MQListMap.Node<Integer,V>  currentDownDisk;
    ReentrantLock reentrantLock ;

    public void setClazz(Class<V> clazz) {
        Clazz = clazz;
    }

    Class<V> Clazz;
    public void setMappedView(MappedView mappedView) {
        this.mappedView = mappedView;
    }
    public MQListMap(){
        super();
    }
    public MQListMap(String filename, Class<V> clazz) throws IOException {
        super();
        this.mappedView = new MappedView(filename);
        reentrantLock =new ReentrantLock();
        Clazz = clazz;
    }
    public MQListMap(String filename) throws IOException, ClassNotFoundException {
        super();
        this.mappedView = new MappedView(filename);
        Clazz= (Class<V>) Class.forName(mappedView.getMapInfo().info);
        reentrantLock =new ReentrantLock();
    }

    public MQListMap(String filename, String name) throws IOException {
        super();
        String info =String.class.getName();
        this.mappedView = new MappedView(filename,name,info);
        reentrantLock =new ReentrantLock();
    }
    protected Integer  creatId() {
        Integer valueIndex;
        //局部加锁保证唯一递增
        synchronized (adder) {
            //读写同步
            valueIndex = adder.intValue();
            adder.increment();
        }
        return valueIndex;
    }
    public V creat(V value) {
        if(null==value)return null;
        V v =super.put(EasyFastFormat.uint32(creatId()), value);
        //nextId=++nextId<0?0:nextId;
        return v;
    }
    public MQListMap.Node<Integer,V> getFirst(){
        V v;
        EasyFastSkipListMap.Node<Integer,V> n;
        for (n = findFirst(); n != null; n = n.next) {
            if ((v = n.getValidValue()) != null)
               return n;
        }
        return n;
    }
    public MQListMap.Node<Integer,V> getFirst(EasyFastSkipListMap.Node<Integer,V> n){
        //V v;
        for (n = n.next; n != null; n = n.next) {
            if ((/*v = */n.getValidValue()) != null)
                return n;
        }
        return n;
    }

    public MQListMap.Node<Integer,V> getFirst(EasyFastSkipListMap.Node<Integer,V> n,Integer idKey){
//        V v;
        for (n = n.next; n != null; n = n.next) {
            if ((/*v =*/ n.getValidValue()) != null&&idKey.equals(n.key)){
                return n;}else if(idKey<n.key) {
                break;
            }
        }
        return n;
    }
    void  pickupDiskDateAll() throws IOException {
        int nextColindex= mappedView.getIntPrevColumnIndex();
        do{
            MappedView.MapColumn column=   new MappedView.MapColumn(mappedView.randomAccessFile,nextColindex);
            int id=column.getId();

            if(id>=0){
                byte[] bytes= MappedView.MapColumn.getColDataBinary(mappedView.randomAccessFile,nextColindex);
                V v= (V) SerializationUtils.deserialize(bytes,Clazz);
                super.put(id,v);
            }
            nextColindex=column.getNextColumnIndex();
        }
        while (nextColindex>0);

    }
     int  tryDownDiskOne(){
       if( reentrantLock.tryLock()) {
           try {
               MQListMap.Node<Integer,V> next;
               if(currentDownDisk!=null){
                   next= getFirst(currentDownDisk);
                   if(next==null)return -3;
                   currentDownDisk=next;
                   currentDownDisk.columnIndex=mappedView.put(currentDownDisk.key, SerializationUtils.serialize(currentDownDisk.getValidValue()));
                   return currentDownDisk.key;
               }else {
                    next= getFirst();
                   if(next==null)return -1;
                   currentDownDisk=next;
                   currentDownDisk.columnIndex=mappedView.put(currentDownDisk.key, SerializationUtils.serialize(currentDownDisk.getValidValue()));
                   return currentDownDisk.key;
               }
           }finally {

               reentrantLock.unlock();
           }

       }else {
           return -2;
       }

     }
    int  downDiskOne(){
            MQListMap.Node<Integer,V> next;
            reentrantLock.lock();
            try {
                if(currentDownDisk!=null){

                next= getFirst(currentDownDisk);
                if(next==null)return -3;
                currentDownDisk=next;


                }else {

                next= getFirst();
                if(next==null)return -1;
                currentDownDisk=next;

                }
            }finally {
                reentrantLock.unlock();
            }

        next.columnIndex=mappedView.put(next.key, SerializationUtils.serialize(next.getValidValue()));
        return next.key;
    }
    void   downDiskAll(){
        if( reentrantLock.tryLock()) {
            try {
                long len=mappedView.channel.size();
                int putIndex=mappedView.getIntPutIndex();
                int prevColumnIndex=mappedView.getIntPrevColumnIndex();
                while (len-mappedView.putIndex <8*1024*1024){
                    len+=8*1024*1024;
                }
                MappedByteBuffer mappedByteBuffer = mappedView.channel.map(FileChannel.MapMode.READ_WRITE, 0,len);

                MQListMap.Node<Integer,V> next=null;
                do{
                    if(currentDownDisk!=null){
                        next= getFirst(currentDownDisk);
                        if(next==null)break;
                        currentDownDisk=next;

                        byte[] bytes=SerializationUtils.serialize(next.getValidValue());
                        int writeIndex=0;
                        //物理磁盘上下一行数据起始下标占位
                        for(int i=MappedView.MapColumn.columnNextColLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(prevColumnIndex++, (byte)( putIndex>>>(i*8)));
                        }
                        next.columnIndex=prevColumnIndex=putIndex;//+bytes.length+MappedView.MapColumn.columnInfoLen;\
                        putIndex+=MappedView.MapColumn.columnNextColLenIndex;
                        //数据行长度
                        for(int i=MappedView.MapColumn.columnLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( bytes.length>>>(i*8)));
                        }
                        //数据id
                        for(int i=MappedView.MapColumn.columnIdLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( next.key>>>(i*8)));
                        }
                        //数据类型默认
                        for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( 0));
                        }
                        for(int i=0;i<bytes.length;i++){
                            mappedByteBuffer.put(putIndex++, bytes[i]);
                        }

                    }
                    else {
                        next= getFirst();
                        if(next==null)break;
                        currentDownDisk=next;
                        byte[] bytes=SerializationUtils.serialize(next.getValidValue());
                        int writeIndex=0;
                        //物理磁盘上下一行数据起始下标占位
                        for(int i=MappedView.MapColumn.columnNextColLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(prevColumnIndex++, (byte)( putIndex>>>(i*8)));
                        }
                        next.columnIndex=prevColumnIndex=putIndex;//+bytes.length+MappedView.MapColumn.columnInfoLen;\
                        putIndex+=MappedView.MapColumn.columnNextColLenIndex;
                        //数据行长度
                        for(int i=MappedView.MapColumn.columnLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( bytes.length>>>(i*8)));
                        }
                        //数据id
                        for(int i=MappedView.MapColumn.columnIdLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( next.key>>>(i*8)));
                        }
                        //数据类型默认
                        for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( 0));
                        }
                        for(int i=0;i<bytes.length;i++){
                            mappedByteBuffer.put(putIndex++, bytes[i]);
                        }
//                        public  static final  int columnInfoLen=12;
//                        public  static final int columnNextColLenIndex=4;//物理磁盘上下一行数据起始下标占位
//                        public  static final int columnLenIndex=3;//数据行长度
//                        public  static final int columnIdLenIndex=4;//数据id占位
////                        public  static final int columnInfoTypeLenIndex=1;//数据类型，json，二进制，gzip，zip，7z等，JDK deflate，LZ4压缩算法，Snappy
//                        mappedByteBuffer.put()
//                        next.columnIndex=mappedView.put(next.key, ));
                    }
                    if(len-putIndex <8*1024*1024){
                        len+=8*1024*1024;
                        mappedByteBuffer = mappedView.channel.map(FileChannel.MapMode.READ_WRITE, 0,len);
                    }

                }while (true);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {

                reentrantLock.unlock();
            }

        }
    }

}
