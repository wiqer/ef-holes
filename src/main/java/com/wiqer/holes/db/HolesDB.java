package com.wiqer.holes.db;

import com.wiqer.holes.core.LongListMap;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HolesDB <K,V>{
    ConcurrentMap<K,Long> indexKeyMap=new ConcurrentHashMap<>();
    LongListMap<Pair<K,V>> valueListMap;
    public HolesDB(String filename) throws IOException {
        valueListMap= new LongListMap<Pair<K, V>>(filename, (Class<Pair<K, V>>) new Pair<K,V>(null,null).getClass());
    }
    public V put(K key, V val){
        V value=null;
        if((value=get(key))!=null){
            Long id= indexKeyMap.get(key);
            valueListMap.removeFromId(id);
            indexKeyMap.remove(key);
            indexKeyMap.put(key, valueListMap.add(new Pair<K,V>(key, val)));
            return value;
        }
        indexKeyMap.put(key, valueListMap.add(new Pair<K,V>(key, val)));
        return val;

    }
    public V get(K key){
        Long idKey;
        V val = null;
        if((idKey=  indexKeyMap.get(key))!=null){
            Pair<K,V> pair;
            if((pair=valueListMap.getFromId(idKey))!=null){
                val=pair.getObjectV();
            }
        }
        return val;
    }
    public V delete(K key){
        V value=null;
        if((value=get(key))!=null){
            Long id= indexKeyMap.get(key);
            valueListMap.removeFromId(id);
            indexKeyMap.remove(key);

        }
        return value;
    }
    public void CleanUP(){
        valueListMap.downDiskAllSegment();
    }
    public void init(){
       valueListMap.pickupDiskDateAllSegment();
       valueListMap.forEach((key, value)->{
           indexKeyMap.put(value.getObjectK(),key);
       });
    }
}
