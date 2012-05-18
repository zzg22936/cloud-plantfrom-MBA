package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FrequentFirstMapper extends 
	Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key1, Text value1, Context context) 
		throws IOException, InterruptedException{
			String strline = value1.toString();
			StringTokenizer itr = new StringTokenizer(strline);
			while(itr.hasMoreTokens()){
				Text oneitem = new Text(itr.nextToken());
				context.write(oneitem, one);
			}			
		}
	
}
