package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class AprioriKReducer extends
	Reducer<Text,Text, Text,Text>{ 
	
	private static Set<Long> sets = new HashSet<Long>();
//	private static final IntWritable totalItemsPaired = new IntWritable();
	private static long transnum = 0;
	private static float minsup = 0;
	private static int k_v = 0;
	private static Counter cf = null;
	private static Counter ct = null;
	private static Counter RF= null;	
	private static Counter r_right= null;
	private static Counter r_wrong= null;
	private int glob = 0;
//	Logger Log = Logger.getLogger(AprioriKReducer.class);
	
	protected void setup(Context context) throws IOException,
     InterruptedException {
		super.setup(context);
		System.out.println("Reduce begin");
		Configuration conf = context.getConfiguration();
		k_v = Integer.parseInt(conf.get("k_value"));
		transnum = Long.parseLong(conf.get("TransNum"));
		minsup = Float.parseFloat(conf.get("MinSup"));
		if(transnum == 0){
			throw new InterruptedException("事务数为0，不能作为除数");
		}
		 cf = context.getCounter("FileWrite", "apri_w");
		  cf.setValue(0);
		  ct = context.getCounter("FileWrite","tran_w");
		  ct.setValue(0);
		  RF = context.getCounter("ReduceNum","TransSize");
		  RF.setValue(0);
		  r_right = context.getCounter("Reduce","Right");
		  r_right.setValue(0);
		  r_wrong = context.getCounter("Reduce","Wrong");
		  r_wrong.setValue(0);
		
 }
	 
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException,InterruptedException,NumberFormatException{
		int sum = 0;
		Iterator<Text> iter = values.iterator();
		Set<Long> tmp_set = new HashSet<Long>();
		String tmp = null;
		String []ss = null;
		while(iter.hasNext()){
	//		byte[] bytes= iter.next().getBytes();
	//		tmp  =new String(bytes,"UTF-8");//改为默认编码
			tmp = iter.next().toString();
			 ss = tmp.split("\t");
			 int  num = 0;
			 num = Integer.parseInt(ss[0]);
			sum += num;
	
			if(ss.length!= num+1){
	//			System.err.println("出错"+"ss.length:"+ss.length+";num"+num);
				r_wrong.increment(1);
			}
			else{
				r_wrong.increment(1);
			}
			for(int i=1;i<num;i++){
				String s_i = ss[i];
				if(s_i.equals("")){
					if(glob == 0){
						glob++;
						System.out.println(tmp);
					}
				//	System.err.println("加入空串");
					break;
				}else{
					long a_t = Long.parseLong(s_i);
					tmp_set.add(a_t);
				}
			}
	//		bytes= null;
			tmp = null;
			ss = null;
		}
	
		float sup = sum/(float)transnum;
	//	Log.info(key.toString()+"sup:"+sup);
		if(sup >=minsup ){
			if(tmp_set.size()>=1000){
				RF.increment(1);
			}
			cf.increment(1);
			context.write(key, new Text(""+sum));
			sets.addAll(tmp_set);
			System.out.println("add one into reduce");
	//		System.out.println(tmp_set);
		}
		tmp_set.clear();
		tmp_set = null;
		iter = null;
		System.out.println("reduce over");
	}
	
	/*
	 * 
	 */
	 protected void cleanup(Context context)
	 	throws IOException, InterruptedException {
		 System.out.println("Reduce 结束;开始写入文件");
		 	 if(writetofile(context)){
		 		 System.out.println("写入文件正确,有用行数为"+sets.size());
		 	 }
		 	 sets.clear();
	 	}
	 
	 private boolean writetofile(Context context)
	 	throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		String outpath = conf.get("outpath");
		Path inputPath = new Path(outpath + "/pretrans"+ k_v);
		FileSystem fs = inputPath.getFileSystem(context.getConfiguration());
		if(fs.exists(inputPath)){
			System.err.println("写入路径已存在");
			return false;
		}
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, inputPath, NullWritable.class,
					LongWritable.class, CompressionType.NONE);
			try{
				NullWritable key = NullWritable.get();
				LongWritable val = new LongWritable();
				for(long ls:sets){
					val.set(ls);
					writer.append(key, val);
				}
				ct.increment(sets.size());
			}catch (Exception e) {
				System.err.println(e.getMessage());
				return false;
			}finally{
				writer.close();
			}
		}catch (Exception e) {
			System.err.println(e.getMessage());
			return false;
		}	
		 return true;
	}
}
