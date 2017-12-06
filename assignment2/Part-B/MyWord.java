package org.apache.storm.starter.bolt;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MyWord implements Serializable, Comparable<MyWord> {
	String _word;
	int _count;
	public MyWord(String word) {
	    _word = word;
	    _count = 1;	    
	}
	public void incrementCount() {
	    ++_count;
	}
	public int getCount() {
	    return _count;
	}
	public String getWord() {
	    return _word;
	}
	@Override
	public int compareTo(MyWord other) {
	    return other.getCount() - this._count;	    
	}
}
