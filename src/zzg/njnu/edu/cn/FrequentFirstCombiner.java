package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentFirstCombiner extends
	Reducer<Text, IntWritable, Text, IntWritable> {//参数可改
	
	private static final IntWritable totalItemsPaired = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException,InterruptedException{
		int sum = 0;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()){
			sum += iter.next().get();
		}
		totalItemsPaired.set(sum);
		context.write(key, totalItemsPaired);
	}
}
