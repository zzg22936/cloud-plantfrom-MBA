package zzg.njnu.edu.cn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class TransStat {
	
	protected static class TransStatMapper
			extends Mapper<LongWritable, Text, Text,LongWritable>{
		public void map(LongWritable key1, Text value1, Context context) 
			throws IOException, InterruptedException{
		}
	}


}
