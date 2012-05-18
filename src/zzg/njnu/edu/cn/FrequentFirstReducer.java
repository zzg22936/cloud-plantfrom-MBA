package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentFirstReducer extends
	Reducer<Text, IntWritable, Text, IntWritable>{  //这边参数要改
	
	private static final IntWritable totalItemsPaired = new IntWritable();
	private static long transnum = 0;
	private static float minsup = 0;
	protected void setup(Context context) throws IOException,
    InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		transnum = Long.parseLong(conf.get("TransNum"));
		minsup = Float.parseFloat(conf.get("MinSup"));
		if(transnum == 0){
			throw new InterruptedException("事务数为0，不能作为除数");
		}
	}
	public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException,InterruptedException{
		int sum = 0;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()){
			sum += iter.next().get();
		}
		///**
		if(sum/(float)transnum >=minsup ){
			totalItemsPaired.set(sum);
			context.write(key, totalItemsPaired);	
		}

	}
}
