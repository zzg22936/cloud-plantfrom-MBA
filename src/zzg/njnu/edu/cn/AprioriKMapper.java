package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class AprioriKMapper extends 
	Mapper<LongWritable, Text, Text, Text>{
	Logger LOG = Logger.getLogger(AprioriKMapper.class);
	HashSet<LongWritable> result = new HashSet<LongWritable>();
//	private final List<String> Lsets = new LinkedList<String>();
//	private final List<String> Csets = new LinkedList<String>();
	private int kv = 0;
	private static Counter ct= null;
	private static Counter Fn = null;
	protected void setup(Context context) throws IOException,
    	InterruptedException {
	  super.setup(context);
	  ct = context.getCounter("FileRead", "Fr");
	  ct.setValue(0);
	  Configuration conf = context.getConfiguration();
	  kv = Integer.parseInt(conf.get("k_value"));
	  readfromfile(context);
	  Fn = context.getCounter("MapFailed","Cross");
//	  getksetsfromfile(context);//获得L(k-1);
	  //根据L(k-1)生成Ck;
//	  apriori_gen();
	  
	}
	/*
	 * 根据ck求其频率
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key1, Text value1, Context context) 
		throws IOException, InterruptedException{
		if(!result.contains(key1)&&kv>2)
			return;
		String linesString = value1.toString();
		StringTokenizer itr= new StringTokenizer(linesString);
		List<String> ls = new LinkedList<String>();
		while (itr.hasMoreTokens()) {
			ls.add((String) itr.nextToken());
		}
	
		Collections.sort(ls);
		if(ls.size()<kv)
			return;
		List<List<String>> u = new LinkedList<List<String>>() ;
		List<String> rList = new LinkedList<String>();
		doselfCross(ls, rList,u, ls.size(), 0 , kv);
	    for(int i=0;i<u.size();i++){
	    	List<String> sList= u.get(i);
	    	if(sList.size()!=kv){
	    		System.err.println("项集不一致");
	    		Fn.increment(1);
	    		return;
	    	}
	
	    		StringBuilder item_pair = new StringBuilder(sList.get(0));
	    		for (int j = 1; j < sList.size(); j++) 
	    			item_pair.append("\t"+sList.get(j));
				item_pair.append(":1");
	    		context.write(new Text(item_pair.toString()), new Text(key1.toString()));
	    }
	}
	
	protected void cleanup(Context context) throws IOException,
		InterruptedException{
	
//	  Lsets.clear();
//	  Csets.clear();
		result.clear();
	  super.cleanup(context);
	}
	
 	protected static void doselfCross(List<String> list,List<String> reslist,
			List<List<String>> u,int lenth,int start, int left){
		if (left == 0) {
			u.add(reslist);
			return;
		}
		if(start + left >lenth ||start >= lenth )
			return ;
		//保留已经加入k项集到商品
		List<String> lStrings = new LinkedList<String>();
		lStrings.addAll(reslist);
		//添加一个新商品 a
		reslist.add(list.get(start));
		// 继续添加
		doselfCross(list, reslist,u, lenth, start + 1, left -1);
	    //不添加商品a ,继续添加
		doselfCross(list, lStrings, u, lenth, start+1, left);
	
	}
 	
	private void readfromfile(Context context) 
		  throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		String outpath = conf.get("outpath");
		Path inputPath= new Path(outpath+"/pretrans"+(kv-1));
		FileSystem fs= inputPath.getFileSystem(conf);
		if (fs.exists(inputPath) == false){
			return ; //求2项集时可能没有
		}
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, inputPath, conf);
			NullWritable key = NullWritable.get();
			LongWritable val = new LongWritable();
			while (reader.next(key,val)) {
				LongWritable newval = new LongWritable(val.get());
				result.add(newval);				
			}
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}finally{
			reader.close();
		}
		LOG.error(kv+"result size:"+ result.size());
	}
 	
}
