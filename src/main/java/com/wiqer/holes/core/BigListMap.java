package com.wiqer.holes.core;



import com.wiqer.holes.util.EasyFastFormat;
import com.wiqer.holes.util.SerializationUtils;
import com.wiqer.holes.util.io.ByteArrayInputPool;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 默认单线程使用
 */
public class BigListMap<V> extends EasyFastSkipListMap<BigInteger,V> {


    public static int shiftBit =26;//28;
    //volatile  Integer nextId=new Integer(0);
    //Call[]分段CAS+@sun.misc.Contended线程可见性，遍历获取缓存行，并非一条指令读，无法保证强一致性
    final LongAdder adder=new LongAdder();
    //持久化
    BigMappedView mappedView ;
    BigListMap.Node<BigInteger,V>  currentDownDisk;
    ReentrantLock reentrantLock ;

    public void setClazz(Class<V> clazz) {
        Clazz = clazz;
    }

    Class<V> Clazz;
    public void setMappedView(BigMappedView mappedView) {
        this.mappedView = mappedView;
    }
    public BigListMap(){
        super();
    }
    public BigListMap(String filename,Class<V> clazz) throws IOException {
        super();
        this.mappedView = new BigMappedView(filename);
        reentrantLock =new ReentrantLock();
        Clazz = clazz;
    }
    public BigListMap(String filename) throws IOException, ClassNotFoundException {
        super();
        this.mappedView = new BigMappedView(filename);
        Clazz= (Class<V>) Class.forName(mappedView.getMapInfo().info);
        reentrantLock =new ReentrantLock();
    }

    public BigListMap(String filename,String name) throws IOException {
        super();
        String info =String.class.getName();
        this.mappedView = new BigMappedView(filename,name,info);
        reentrantLock =new ReentrantLock();
    }
    protected Integer  creatId() {
        int valueIndex;
        //局部加锁保证唯一递增
        synchronized (adder) {
            //读写同步
            valueIndex = adder.intValue();
            adder.increment();
        }
        return valueIndex;
    }
    protected long  creatIdLong() {
        long valueIndex;
        //局部加锁保证唯一递增
        synchronized (adder) {
            //读写同步
            valueIndex = adder.longValue();
            adder.increment();
        }
        return valueIndex;
    }
    public V creat(V value) {
        if(null==value)return null;
        V v =super.put(BigInteger.valueOf(creatIdLong()), value);
        //nextId=++nextId<0?0:nextId;
        return v;
    }
    public BigListMap.Node<BigInteger,V> getFirst(){
        V v;
        EasyFastSkipListMap.Node<BigInteger,V> n;
        for (n = findFirst(); n != null; n = n.next) {
            if ((v = n.getValidValue()) != null)
                return n;
        }
        return n;
    }
    public BigListMap.Node<BigInteger,V> getFirst(EasyFastSkipListMap.Node<BigInteger,V> n){
        //V v;
        for (n = n.next; n != null; n = n.next) {
            if ((/*v = */n.getValidValue()) != null)
                return n;
        }
        return n;
    }

    public BigListMap.Node<BigInteger,V> getFirst(EasyFastSkipListMap.Node<BigInteger,V> n,BigInteger idKey){
//        V v;
        for (n = n.next; n != null; n = n.next) {
            if ((/*v =*/ n.getValidValue()) != null&&idKey.equals(n.key)){
                return n;}else if(idKey.longValue()<n.key.longValue()) {
                break;
            }
        }
        return n;
    }

    public long sizeLong() {
        long count = 0;
        for (EasyFastSkipListMap.Node<BigInteger,V> n = findFirst(); n != null; n = n.next) {
            if (n.getValidValue() != null)
                ++count;
        }
        return count;
    }
    public void  pickupDiskDateAll() throws IOException {
        int nextColindex= mappedView.getIntPutIndex();
        do{
            MappedView.MapColumn column=   new MappedView.MapColumn(mappedView.randomAccessFile,nextColindex);
            BigInteger id=BigInteger.valueOf(column.getId());

            if(id.longValue()>=0){
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
                BigListMap.Node<BigInteger,V> next;
                if(currentDownDisk!=null){
                    next= getFirst(currentDownDisk);
                    if(next==null)return -3;
                    currentDownDisk=next;
                    currentDownDisk.columnIndex=mappedView.put(currentDownDisk.key.intValue(), SerializationUtils.serialize(Objects.requireNonNull(currentDownDisk.getValidValue())));
                    return currentDownDisk.key.intValue();
                }else {
                    next= getFirst();
                    if(next==null)return -1;
                    currentDownDisk=next;
                    currentDownDisk.columnIndex=mappedView.put(currentDownDisk.key.intValue(), SerializationUtils.serialize(Objects.requireNonNull(currentDownDisk.getValidValue())));
                    return currentDownDisk.key.intValue();
                }
            }finally {

                reentrantLock.unlock();
            }

        }else {
            return -2;
        }

    }
    public int  downDiskOne(){
        BigListMap.Node<BigInteger,V> next;
        reentrantLock.lock();
        if(currentDownDisk!=null){
            try {
                next= getFirst(currentDownDisk);
                if(next==null)return -3;
                currentDownDisk=next;
            }finally {
                reentrantLock.unlock();
            }

        }else {
            try {
                next= getFirst();
                if(next==null)return -1;
                currentDownDisk=next;
            }finally {
                reentrantLock.unlock();
            }
        }
        next.columnIndex=mappedView.put(next.key.intValue(), SerializationUtils.serialize(Objects.requireNonNull(next.getValidValue())));
        return next.key.intValue();
    }
    public void   downDiskAll(){
        if( reentrantLock.tryLock()) {
            try {
                long len=mappedView.channel.size();
                int putIndex=mappedView.getIntPutIndex();
                int prevColumnIndex=mappedView.getIntPrevColumnIndex();
                while (len-mappedView.putIndex <8*1024*1024){
                    len+=8*1024*1024;
                }
                MappedByteBuffer mappedByteBuffer = mappedView.channel.map(FileChannel.MapMode.READ_WRITE, 0,len);

                BigListMap.Node<BigInteger,V> next=null;
                do{
                    if(currentDownDisk!=null){
                        next= getFirst(currentDownDisk);
                        if(next==null)break;
                        currentDownDisk=next;

                        byte[] bytes=SerializationUtils.serialize(Objects.requireNonNull(next.getValidValue()));
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
                            mappedByteBuffer.put(putIndex++, (byte)( next.key.intValue()>>>(i*8)));
                        }
                        //数据类型默认
                        for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( 0));
                        }
                        for (byte aByte : bytes) {
                            mappedByteBuffer.put(putIndex++, aByte);
                        }

                    }
                    else {
                        next= getFirst();
                        if(next==null)break;
                        currentDownDisk=next;
                        byte[] bytes=SerializationUtils.serialize(Objects.requireNonNull(next.getValidValue()));
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
                            mappedByteBuffer.put(putIndex++, (byte)( next.key.intValue()>>>(i*8)));
                        }
                        //数据类型默认
                        for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                            mappedByteBuffer.put(putIndex++, (byte)( 0));
                        }
                        for (byte aByte : bytes) {
                            mappedByteBuffer.put(putIndex++, aByte);
                        }
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

    public void   downDiskAllSegment(){
        if( reentrantLock.tryLock()) {
            try {
                BigListMap.Node<BigInteger,V> next=currentDownDisk;
                long segmentId=-1;
                long segmentGroupHead=-1;
                Segment:
                while (segmentId!=(mappedView.putIndex>>shiftBit)) {
                    //要后28位
                    segmentId=(mappedView.putIndex>>shiftBit);
                    segmentGroupHead=segmentId<<shiftBit;
                    RandomAccessFile randomAccessFile = new RandomAccessFile(mappedView.fileName+"_"+segmentId+MappedView.suffix, "rw");
                    FileChannel channel= randomAccessFile.getChannel();
                    long len=channel.size();
                    int putIndex= EasyFastFormat.uintNBit(mappedView.getIntPutIndex(),shiftBit) ;
                    int prevColumnIndex= EasyFastFormat.uintNBit(mappedView.getIntPrevColumnIndex(),shiftBit) ;
                    int nextColumnIndex=0;
                    if (len-putIndex < 1L <<(shiftBit-2)){
                        len= (long) putIndex + 1 <<(shiftBit-2);
                    }
                    MappedByteBuffer mappedByteBuffer =channel.map(FileChannel.MapMode.READ_WRITE, 0,len);


                    do{
                        if(next!=null){
                            next= getFirst(next);
                            if(next==null){
                                break Segment;
                            }
                            byte[] bytes=SerializationUtils.serialize(Objects.requireNonNull(next.getValidValue()));
                            //nextColumnIndex
                            nextColumnIndex=putIndex+MappedView.MapColumn.columnInfoLen+bytes.length;
                            mappedView.putIndex=next.columnIndex=putIndex+segmentGroupHead;//+bytes.length+MappedView.MapColumn.columnInfoLen;\
                            //物理磁盘下一行数据起始下标占位
                            for(int i=MappedView.MapColumn.columnNextColLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( nextColumnIndex>>>(i*8)));
                            }
                            //数据行长度
                            for(int i=MappedView.MapColumn.columnLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( bytes.length>>>(i*8)));
                            }
                            //数据id
                            for(int i=MappedView.MapColumn.columnIdLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( next.key.intValue()>>>(i*8)));
                            }
                            //数据类型默认
                            for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( 0));
                            }
                            for (byte aByte : bytes) {
                                mappedByteBuffer.put(putIndex++, aByte);
                            }

                        }
                        else {
                            next= getFirst();
                            if(next==null){
                                break Segment;
                            }
                            byte[] bytes=SerializationUtils.serialize(Objects.requireNonNull(next.getValidValue()));
                            nextColumnIndex=putIndex+MappedView.MapColumn.columnInfoLen+bytes.length;
                            //物理磁盘上下一行数据起始下标占位
                            mappedView.putIndex= next.columnIndex=(putIndex)+segmentGroupHead;//+bytes.length+MappedView.MapColumn.columnInfoLen;\
                            for(int i=MappedView.MapColumn.columnNextColLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( nextColumnIndex>>>(i*8)));
                            }
                            //数据行长度
                            for(int i=MappedView.MapColumn.columnLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( bytes.length>>>(i*8)));
                            }
                            //数据id
                            for(int i=MappedView.MapColumn.columnIdLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( next.key.intValue()>>>(i*8)));
                            }
                            //数据类型默认
                            for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                                mappedByteBuffer.put(putIndex++, (byte)( 0));
                            }
                            for (byte aByte : bytes) {
                                mappedByteBuffer.put(putIndex++, aByte);
                            }
                        }
                        if(putIndex>(1<<shiftBit)){
                            mappedView.putIndex=putIndex+segmentGroupHead;
                            break ;

                        }
                        if(len-putIndex < 1L <<(shiftBit-3)){
                            len+= 1L <<(shiftBit-3);
                            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0,len);
                        }

                    }while (true);

                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {

                reentrantLock.unlock();
            }

        }
    }
    public void  pickupDiskDateAllSegment() throws IOException {
        if( reentrantLock.tryLock()) {
            try {
                ByteArrayInputPool bytePool=new ByteArrayInputPool();
                BigListMap.Node<BigInteger,V> currtNode=findFirst();

                long segmentId=-1;
                long segmentGroupHead=-1;
                long idMakeUp=0;

                long prevId=0;
                Segment:
                while (segmentId!=(mappedView.putIndex>>shiftBit)) {
                    //要后28位
                    segmentId=(mappedView.putIndex>>shiftBit);
                    segmentGroupHead=segmentId<<shiftBit;
                    RandomAccessFile randomAccessFile = new RandomAccessFile(mappedView.fileName+"_"+segmentId+MappedView.suffix, "r");
                    FileChannel channel= randomAccessFile.getChannel();
                    long len=channel.size();
                    int putIndex= EasyFastFormat.uintNBit(mappedView.getIntPutIndex(),shiftBit) ;
//                    int prevColumnIndex= EasyFastFormat.uintNBit(mappedView.getIntPrevColumnIndex(),shiftBit) ;
                    int nextColumnIndex;
                    long id;
                    int dataLen;
                    MappedByteBuffer mappedByteBuffer =channel.map(FileChannel.MapMode.READ_ONLY, 0,len);


                    do{

                        //nextColumnIndex
                        nextColumnIndex=0;
                        id=0;
                        dataLen=0;
                        //mappedView.putIndex=next.columnIndex=putIndex+segmentGroupHead;//+bytes.length+MappedView.MapColumn.columnInfoLen;\
                        //物理磁盘下一行数据起始下标占位
                        for(int i=MappedView.MapColumn.columnNextColLenIndex-1;i>=0;i--){
                            nextColumnIndex+= mappedByteBuffer.get(putIndex++)<<(8*i);
                        }
                        //数据行长度
                        for(int i=MappedView.MapColumn.columnLenIndex-1;i>=0;i--){
                            dataLen+= mappedByteBuffer.get(putIndex++)<<(8*i);
                        }
                        //数据id
                        for(int i=MappedView.MapColumn.columnIdLenIndex-1;i>=0;i--){
                            id+= (long) mappedByteBuffer.get(putIndex++) <<(8*i);;
                        }
                        if(id<=0&&dataLen==0){
                            break Segment;
                        }
                        //保持id递增
                        id+=idMakeUp;
                        while (id<prevId){
                            id-= idMakeUp;
                            idMakeUp=prevId-id+1;
                            id+= idMakeUp;
                        }
                        prevId=id;
                        bytePool.reset(0,dataLen);
                       // byte[] bytes=new byte[dataLen];
                        //数据类型默认
                        for(int i=MappedView.MapColumn.columnInfoTypeLenIndex-1;i>=0;i--){
                            mappedByteBuffer.get(putIndex++);
                        }
//                        for(int i=0;i<bytes.length;i++){
//                            bytes[i]= mappedByteBuffer.get(putIndex++);
//                        }
                        for(int i=0;i<dataLen;i++){
                            bytePool.put( i, mappedByteBuffer.get(putIndex++));
                        }
                        V v= (V) SerializationUtils.deserialize(bytePool,Clazz);
                        currtNode= super.put(currtNode,BigInteger.valueOf(id) ,v);
                        currtNode.columnIndex=putIndex;
                        if(putIndex>(1<<shiftBit)){
                            mappedView.putIndex=putIndex+segmentGroupHead;
                            break ;
                        }

                    }while (true);

                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                reentrantLock.unlock();
            }
        }
    }

}
