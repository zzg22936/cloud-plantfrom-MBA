package zzg.njnu.edu.cn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class AprioriKCombiner extends
	Reducer<Text, Text, Text, Text>{ 
	private static Counter comstatic = null;
//	Logger Log = Logger.getLogger(AprioriKCombiner.class);
		protected void setup(Context context
		) throws IOException, InterruptedException {
			 comstatic = context.getCounter("Combiner", "over500");
			  comstatic.setValue(0);

		}	

		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException,InterruptedException{
			long sum = 0;
			Iterator<Text> iter = values.iterator();
			StringBuilder newvalue = new StringBuilder();
			String keyString = key.toString();
			String []ss = keyString.split(":");
			while(iter.hasNext()){
				if(newvalue.length()!=0)
					newvalue.append("\t");
				newvalue.append(iter.next().toString());
				sum ++;
				if(sum >19){		
					newvalue.insert(0,""+20+"\t");
					String s1 = newvalue.toString();
				//	byte[] tmpbytes = s1.getBytes();
				//	String s_tString = new String(tmpbytes, "UTF-8");
				
			//		context.write(new Text(ss[0]),new Text(s_tString));
					context.write(new Text(ss[0]),new Text(s1));	
					comstatic.increment(1);
					s1 = null;
				//	tmpbytes = null;
				//	s_tString = null;
					newvalue =null;
					newvalue = new StringBuilder();
					sum = 0; 
				}
			}	
			if(sum>20){
				System.err.println("sum over 20");
			}
			newvalue.insert(0,""+sum+"\t");
	//		byte[] bytes = newvalue.toString().getBytes("UTF-8");
	//		String s_tString = new String(bytes, "UTF-8");
			String s_tString = newvalue.toString();
			context.write(new Text(ss[0]),new Text(s_tString));
	//		bytes =null;
			System.out.println(ss[0]+":"+s_tString);
			s_tString = null;
			newvalue = null;
			iter = null;
	//		System.out.println("combine one over");
		}
		
		/*
		 * 
		 */
	 protected void cleanup(Context context)
	 	throws IOException, InterruptedException {
		 	 System.out.println("combine over");
	 	}
}
