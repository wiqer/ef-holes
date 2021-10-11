package com.wiqer.holes;

import com.wiqer.holes.db.HolesDB;
import org.junit.Test;

import java.io.IOException;

public class HolesDBTest {
    @Test
    public void HolesDBPutTest() throws IOException {
        HolesDB<String,String> holes=new HolesDB<>("H:/HolesDB/my");
        for (int i = 0; i <300 ; i++) {
             holes.put(""+i,"das"+i);
        }
        holes.CleanUP();
    }
    @Test
    public void HolesDBLoadTest() throws IOException {
        HolesDB<String,String> holes=new HolesDB<>("H:/HolesDB/my");
        holes.init();
        String val=holes.get(""+267);
        System.out.println(val);
    }
    @Test
    public void HolesDBPutPressureTest() throws IOException {
        HolesDB<String,String> holes=new HolesDB<>("H:/HolesDB/my");
        for (int i = 0; i <1000000 ; i++) {
            holes.put(""+i,"HolesDB写数据压测 100w条数据 程序存机械硬盘速度运行时间"+i);
        }
        long startTime = System.currentTimeMillis();    //获取开始时间
        holes.CleanUP();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("HolesDB写数据压测 100w条数据 程序存机械硬盘速度运行时间：" + (endTime - startTime)+"ms");

    }
    @Test
    public void HolesDBLoadPressureTest() throws IOException {
        HolesDB<String,String> holes=new HolesDB<>("H:/HolesDB/my");
        long startTime = System.currentTimeMillis();    //获取开始时间
        holes.init();
        long endTime = System.currentTimeMillis();    //获取结束时间
        System.out.println("HolesDB读数据压测 100w条数据 程序存机械硬盘速度运行时间：" + (endTime - startTime)+"ms");
        String val=holes.get(""+26127);
        System.out.println(val);
    }
}
