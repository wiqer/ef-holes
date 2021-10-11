package com.wiqer.holes;

import com.wiqer.holes.core.MQListMap;
import com.wiqer.holes.util.SerializationUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MQListMapTest {
//    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        //testSerializationUtils();
//        // putData();
//        // getData();
//        //putDataM();
//        putDataAll();
//    }
    @Test
   public void testSerializationUtils(){
        long startTime = System.currentTimeMillis();    //获取开始时间
        for(int i=0;i<100000;i++){
            SerializationUtils.serialize(i+"asd");
        }
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("1000000serialize程序运行时间：" + (endTime - startTime));
        //Raid5输出程序运行时间-》downDiskOneloop程序运行时间：12300：100，0000


    }

//    static void putData() throws IOException {
//        //System.out.println(EasyFastFormat.uint32(-20000000001l));
//        MQListMap<String> map=new MQListMap<>("A:\\test6.efd","test");
//        for(int i=0;i<10000;i++){
//            map.creat(i+"asd");
//        }
////        for(int i=9;i<=100;i++){
////            map.remove(i);
////            // System.out.println(map.get(i));
////        }
////        for(int i=0;i<100;i++){
////             System.out.println(map.getNext(i));
////        }
//        MQListMap.Node<Integer,String> nodetemp=map.getFirst();
//        System.out.println(nodetemp.value);
////        for(int i=0;i<99;i++){
////            System.out.println(map.getNextNode(i).value);
////        }
//        long startTime = System.currentTimeMillis();    //获取开始时间
//        while (map.downDiskOne()>=0);
//        long endTime = System.currentTimeMillis();    //获取结束时间
//        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
//        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
//        //Raid0  442172：100，0000
//
//        System.out.println("??");
//    }
//    static void putDataM() throws IOException {
//        //System.out.println(EasyFastFormat.uint32(-20000000001l));
//        MQListMap<String> map=new MQListMap<>("A:\\test7.efd","test");
//        for(int i=0;i<1000000;i++){
//            map.creat(i+"asd");
//        }
//        long startTime = System.currentTimeMillis();    //获取开始时间
//        ExecutorService executorService = Executors.newCachedThreadPool();
//        for (int i = 1; i <= 10; i++) {
//            int count = i;
//
//            executorService.execute(() -> {
//                while (map.downDiskOne()>=0);
//            });
//            try {
//                Thread.sleep(100L);
//                //Thread.sleep(1000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        executorService.shutdown();
//        executorService.isTerminated();
//        long endTime = System.currentTimeMillis();    //获取结束时间
//        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
//        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
//        //Raid0  442172：100，0000
//
//        System.out.println("??");
//    }
//    static void putDataAll() throws IOException {
//        //System.out.println(EasyFastFormat.uint32(-20000000001l));
//        MQListMap<String> map=new MQListMap<>("A:\\test11.efd","test");
//        for(int i=0;i<10000000;i++){
//            map.creat(i+"asd");
//        }
//        long startTime = System.currentTimeMillis();    //获取开始时间
//        map.downDiskAll();
//        long endTime = System.currentTimeMillis();    //获取结束时间
//        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
//        //输出程序运行时间-》downDiskOneloop程序运行时间：621896：100，0000
//        //Raid0  442172：100，0000
//
//        System.out.println("??");
//    }
//    static void getData() throws IOException, ClassNotFoundException {
//        long startTime = System.currentTimeMillis();    //获取开始时间
//        MQListMap<String> map=new MQListMap<String> ("A:\\test11.efd");
//        map.pickupDiskDateAll();
//        long endTime = System.currentTimeMillis();    //获取结束时间
//        System.out.println("downDiskOneloop程序运行时间：" + (endTime - startTime));
//        for(int i=0;i<9;i++){
//            if(map.get(i)!=null)System.out.println(map.get(i));
//        }
//
//        System.out.println("??");
//    }

}
