package com.wiqer.holes.db;

public class Pair <K,V>  {
    private K object1;
    private V object2;

    public Pair(K object1, V object2) {
        this.object1 = object1;
        this.object2 = object2;
    }

    public K getObjectK() {
        return object1;
    }

    public void setObjectK(K object1) {
        this.object1 = object1;
    }

    public V getObjectV() {
        return object2;
    }

    public void setObjectV(V object2) {
        this.object2 = object2;
    }
}