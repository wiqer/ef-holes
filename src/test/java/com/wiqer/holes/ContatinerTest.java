package com.wiqer.holes;

import org.junit.Test;

public class ContatinerTest {
    public Integer d=6;
    static  Integer c=6;
    @Test
    public  void test(String[] args) {
//        Integer b=3;
//        intToll(b);
//        System.out.println(b);
        ContatinerTest e=new ContatinerTest();
        e.d=156;
        ObjToll(e);
        System.out.println(e.d);
    }
    static void intToll(Integer a){
        a=c++ ;
    }
    static void ObjToll(ContatinerTest a){
        a=new ContatinerTest() ;
    }
}
