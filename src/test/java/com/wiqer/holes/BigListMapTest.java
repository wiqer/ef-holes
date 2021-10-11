package com.wiqer.holes;

import com.wiqer.holes.core.BigListMap;
import com.wiqer.holes.util.SerializationUtils;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BigListMapTest {
    @Test
    public  void main(String[] args) throws IOException, ClassNotFoundException {
//        BigListMap<String> map=new BigListMap();
//        for(int i=0;i<10;i++){
//            map.creat(i+"asd");
//        }
        //testSerializationUtils();
        // putData();
        //getData();
        getDataAllSegment();
        //putDataM();
        //putDataAll();
        // putDataAllSegment();
    }
    static void testSerializationUtils(){
        long startTime = System.currentTimeMillis();    //获取开始时间
        for(int i=0;i<100000;i++){
            SerializationUtils.serialize(i+"asd");
        }
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("1000000serialize程序运行时间：" + (endTime - startTime));
        //Raid5输出程序运行时间-》downDiskOneloop程序运行时间：12300：100，0000


    }

    static void putData() throws IOException {
        //System.out.println(EasyFastFormat.uint32(-20000000001l));
        BigListMap<String> map=new BigListMap("A:\\test6.efd","test");
        for(int i=0;i<10000;i++){
            map.creat(i+"asd");
        }
//        for(int i=9;i<=100;i++){
//            map.remove(i);
//            // System.out.println(map.get(i));
//        }
//        for(int i=0;i<100;i++){
//             System.out.println(map.getNext(i));
//        }
        BigListMap.Node<BigInteger,String> nodetemp=map.getFirst();
//        System.out.println(nodetemp.value);
//        for(int i=0;i<99;i++){
//            System.out.println(map.getNextNode(i).value);
//        }
        long startTime = System.currentTimeMillis();    //获取开始时间
        while (map.downDiskOne()>=0);
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
        //Raid0  442172：100，0000

        System.out.println("??");
    }
     void putDataM() throws IOException {
        //System.out.println(EasyFastFormat.uint32(-20000000001l));
        BigListMap<String> map=new BigListMap("A:\\test7.efd","test");
        for(int i=0;i<1000000;i++){
            map.creat(i+"asd");
        }
        long startTime = System.currentTimeMillis();    //获取开始时间
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 1; i <= 10; i++) {
            executorService.execute(() -> {
                while (map.downDiskOne()>=0);
            });
            try {
                Thread.sleep(100L);
                //Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        executorService.isTerminated();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
        //Raid0  442172：100，0000

        System.out.println("??");
    }

     void putDataAllSegment() throws IOException {
        //System.out.println(EasyFastFormat.uint32(-20000000001l));
        BigListMap<String> map=new BigListMap("A:\\BigMapSegment\\LaserList","test");
        for(int i=0;i<20000000;i++){
            map.creat("asd");
        }
        long startTime = System.currentTimeMillis();    //获取开始时间
        map.downDiskAllSegment();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
        //Raid0  442172：100，0000

        System.out.println("??");
    }
     void putDataAll() throws IOException {
        //System.out.println(EasyFastFormat.uint32(-20000000001l));
        BigListMap<String> map=new BigListMap("A:\\test20.efd","test");
        for(int i=0;i<1000;i++){
            map.creat(i+"asd");
        }
        long startTime = System.currentTimeMillis();    //获取开始时间
        map.downDiskAll();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
        //Raid0  442172：100，0000

        System.out.println("??");
    }
     void getData() throws IOException, ClassNotFoundException {
        long startTime = System.currentTimeMillis();    //获取开始时间
        BigListMap map=new BigListMap<String> ("A:\\test20.efd");
        map.pickupDiskDateAll();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        for(int i=0;i<9;i++){
            if(map.get(BigInteger.valueOf(i))!=null)System.out.println(map.get(BigInteger.valueOf(i)));
        }

        System.out.println("??");
    }
     void getDataAllSegment() throws IOException, ClassNotFoundException {
        long startTime = System.currentTimeMillis();    //获取开始时间
        BigListMap map=new BigListMap<String> ("A:\\BigMapSegment\\LaserList");
        map.pickupDiskDateAllSegment();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
        for(int i=0;i<9;i++){
            if(map.get(BigInteger.valueOf(i))!=null)System.out.println(map.get(BigInteger.valueOf(i)));
        }

        System.out.println("??");
    }
}
