package lab4;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class stage1 {

	public static class stage1_Mapper extends  Mapper<Object, Text, Text, Text>{

		protected void map(Object key,Text value,Context context) throws IOException, InterruptedException
	 	{
	 		String[] strs = value.toString().split(" ");
	 		String minStr = "";
	 		String maxStr = "";
	 		if(strs[0].compareTo(strs[1]) != 0){
	 			minStr = strs[0].compareTo(strs[1])>0?strs[1]:strs[0];
	 			maxStr = strs[0].compareTo(strs[1])>0?strs[0]:strs[1];
	 		}
	 		context.write(new Text(minStr), new Text(maxStr));	 
	 	}
	}
	
	public static class stage1_Reducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			TreeSet<String> strs = new TreeSet<String>();
			for(Text val:values){
				strs.add(val.toString());
			}		
			String str = "";		
			Iterator<String> it=strs.iterator();
		    while(it.hasNext()){
		    	str += it.next()+",";
		    }		    
		    context.write(key, new Text(str.substring(0, str.length()-1)));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    @SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "stage1");
	    job1.setJarByClass(stage1.class);
	    job1.setMapperClass(stage1_Mapper.class);
	    job1.setReducerClass(stage1_Reducer.class);
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    job1.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);
	}
}
