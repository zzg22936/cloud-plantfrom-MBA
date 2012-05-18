package zzg.njnu.edu.cn;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AprioriDirver extends Configured implements Tool{
	
	public int run(String[] args)throws Exception{
		Path inputdataPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		float minsup =(float) Double.parseDouble(args[2]);

		int reducenum = Integer.parseInt(args[3]);
		long stime = System.currentTimeMillis();
		//统计事务数
		long transnum = stattrans(inputdataPath,outPath,reducenum);
		if(transnum == 0){
			System.out.println("事务数为0");
			throw new Exception("查看事务是否读取正确");
		}
		Path outputPath = GetFirstFreq(inputdataPath,outPath,transnum,minsup,reducenum);
		System.out.println("It takes: " + (System.currentTimeMillis() - stime) + " msec to get 1项集");
		
		int k = 2;
		while(true){
			
			//先获得L(k-1)的尺寸,看其是否为0
			long st=System.currentTimeMillis();
			//根据L(k-1)获得C(k)
			Path r_outpath = GetKFreq(inputdataPath, outPath,outputPath, k,transnum,minsup,reducenum); //
			System.out.println("It takes: " + (System.currentTimeMillis() - st) + " msec to get "+ k+"项集");
			if(check(k,outPath)==false)
				break;
			k = k+1;
			outputPath = r_outpath;
			//过滤 获得L(k)		
		}
		System.out.println("It takes: " + (System.currentTimeMillis() - stime) + " msec to solve the problem");
		return 0;
	}
	
	public static long stattrans(Path inputpath,Path outPath,int reducenum)throws
			IOException,InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"TransStat");
		job.setJarByClass(AprioriDirver.class);
		FileInputFormat.setInputPaths(job, inputpath);
		Path outpath = new Path(outPath.toString()+"/tnum");  //out
		FileOutputFormat.setOutputPath(job, outpath);
		job.setMapperClass(TransStat.TransStatMapper.class);
		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true)){
			throw new InterruptedException("stat trans num failed");
		}
	
		Counters counters = job.getCounters();
		org.apache.hadoop.mapreduce.Counter c = counters.findCounter(Task.Counter.MAP_INPUT_RECORDS);
		
		long num = c.getValue();
		return num;
	}
	public static boolean check(int ck,Path outPath)throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem  hdfs = (DistributedFileSystem)fs;
		Path inputPath = new Path(outPath.toString()+"/freq_"+ck);
//		Path inputPath = new Path("/user/zzg/freqsets/freq_"+ck);
		FileStatus[] status = hdfs.listStatus(inputPath);
		long t_size = 0;
		for (FileStatus s:status) {
			t_size+=s.getLen();
		}

		if(t_size == 0)
			return false;
		else 
			return true;
	}
	
	public static Path GetFirstFreq(Path input,Path outPath,long transnum,float minsup,int reducenum) throws 
			IOException,InterruptedException,ClassNotFoundException{
		
		Configuration conf= new Configuration();
		conf.setLong("TransNum", transnum);
		conf.setFloat("MinSup", minsup);
		Job job = new Job(conf,"Freq1set"); 
		job.setJarByClass(AprioriDirver.class);
		FileInputFormat.setInputPaths(job, input);
		Path outputpath = new Path(outPath.toString()+"/freq_1");
		
		FileOutputFormat.setOutputPath(job, outputpath);
			
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(FrequentFirstMapper.class);
		
		job.setCombinerClass(FrequentFirstCombiner.class);
		job.setReducerClass(FrequentFirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(reducenum);
		
		if(!job.waitForCompletion(true)){
			throw new InterruptedException("find 1 set frequent fialed");
		}
		return outputpath;
		
	}
	
	public static Path GetKFreq(Path input,Path outPath,Path prekpath,int k,long transnum,float minsup,int reducenum)throws
			IOException,InterruptedException,ClassNotFoundException{

		Configuration conf= new Configuration();
		conf.setInt("k_value", k);
	//	conf.setInt("MinSup", 20);
		conf.setLong("TransNum", transnum);
		conf.setFloat("MinSup", minsup);
		conf.set("outpath", outPath.toString());
		Job job = new Job(conf,"Freqsetk"+k); 
	
		job.setJarByClass(AprioriDirver.class);
		FileInputFormat.setInputPaths(job, input);
		
		Path outputPath = new Path(outPath.toString()+"/freq_"+k);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Path outdir = new Path(outPath.toString()+"/freq_"+k);
		FileSystem.get(conf).delete(outdir,true);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(AprioriKMapper.class);
		job.setCombinerClass(AprioriKCombiner.class);
		job.setReducerClass(AprioriKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		if(!job.waitForCompletion(true)){
			throw new InterruptedException("find" +k +" set frequent fialed");
		}
		return outputPath;
	}
	public static void main(String[] args)throws Exception{
		ToolRunner.run(new AprioriDirver(), args);
	}
}
