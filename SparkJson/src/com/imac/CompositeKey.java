package com.imac;

import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.BooleanWritable;

public class CompositeKey implements Comparable<CompositeKey> {
	String id;
	BooleanWritable boo = new BooleanWritable();
	private HashMap<String, Double> mHashMap = JavaParseJson.compositeKeyHashMap;
	double count=1.0;
	
	CompositeKey(String term, String b, double count) {
		id = term;
		boo.set(Boolean.parseBoolean(b));
		
		if(mHashMap.size()>0 && isContain(id+b)){
			count+=1.0;
			mHashMap.put(id+b, count);
		}else{
			mHashMap.put(id+b, count);
		}
	}

	public boolean isContain(String string){
		Set<String> keys = mHashMap.keySet();
		for(String v : keys){
			if(v.equals(string)){
				return true;
			}
		}
		return false;
	}
	
	public String getID() {
		return id;
	}

	public BooleanWritable getBoo() {
		return boo;
	}
	
	public Double getDouble(String key) {
		return mHashMap.get(key);
	}

	public int compareTo(CompositeKey other) {
		int ret = -1 * boo.compareTo(other.boo);
		if (ret == 0) {
			ret = id.compareTo(other.id);
		}
		return ret;
	}
}
