package com.wiqer.holes;

import com.wiqer.holes.core.LongListMap;
import org.junit.Test;

import java.io.IOException;

public class LongListMapTest {
    @Test
    public void longListMapTestPut(){
        try {
            LongListMap<String> longListMap=new LongListMap<>("D:/longListMapTest/my",String.class);
            for (long i = 0; i < 1000000; i++) {
                longListMap.put(i,"asdsa"+i);
            }
            //份文件段存储
            long startTime = System.currentTimeMillis();    //获取开始时间
            longListMap.downDiskAllSegment();
            long endTime = System.currentTimeMillis();    //获取结束时间
            System.out.println("1000000 程序存固态硬盘速度运行时间：" + (endTime - startTime)+"ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            LongListMap<String> longListMapSolid=new LongListMap<>("H:/longListMapTest/my",String.class);
            for (long i = 0; i < 1000000; i++) {
                longListMapSolid.put(i,"asdsa"+i);
            }
            //份文件段存储
            long startTime = System.currentTimeMillis();    //获取开始时间
            longListMapSolid.downDiskAllSegment();
            long endTime = System.currentTimeMillis();    //获取结束时间
            System.out.println("1000000 程序存机械硬盘速度运行时间：" + (endTime - startTime)+"ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void longListMapTestLoad(){
        try {
            LongListMap<String> longListMap=new LongListMap<>("D:/longListMapTest/my",String.class);
            longListMap.pickupDiskDateAllSegment();
            System.out.println(longListMap.sizeLong());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
