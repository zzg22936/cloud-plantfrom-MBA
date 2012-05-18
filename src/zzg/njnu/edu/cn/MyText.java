package zzg.njnu.edu.cn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyText implements Writable {
/*
	private Set<Long> set ;
	
	public MyText(){
		set = null;
	}
	
	public MyText(Set<Long> s){
		set.addAll(s);
	}
	
	public MyText(String string)throws NumberFormatException{
		String[]ss=string.split("\t");
		for(String s:ss){
			set.add(Long.parseLong(s));
		}
	}
	*/
	private String textstring;
	public MyText(){
	}
	
	public MyText(String s) {
		textstring =s;
	}
	public void readFields(DataInput in)throws IOException {
		textstring = in.readUTF();
	}
	public void write(DataOutput out)throws IOException{
		out.writeUTF(textstring);
	}
	public int hashCode(){
		return textstring.hashCode();
	}
	
	public boolean equals(Object o){
		if(o instanceof MyText){
			MyText mText = (MyText)o;
			return this.textstring.equals(mText.textstring);
		}
		return false;
	}
	
	public String tostString() {
		return textstring;
	}
	
	
}
