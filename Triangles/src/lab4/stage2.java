package lab4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class stage2 {
	
	static int allNum = 0;

	public static class stage2_Mapper extends Mapper<Object, Text, Text, Text>{
		
		protected void map(Object key,Text value,Context context) throws IOException, InterruptedException
	 	{
	 		String[] strs = value.toString().split("	");
	 		String inGraph = strs[0];
	 		String[] neighs = strs[1].split(",");
	 		for(int i = 0; i < neighs.length; i++){
	 			context.write(new Text(inGraph+","+neighs[i]), new Text("1"));
	 		}
	 		for(int i = 0; i < neighs.length; i++)
	 			for(int j = i+1;j < neighs.length; j++)					
						context.write(new Text(neighs[i]+","+neighs[j]), new Text("0"));
	 	}
	}
	
	public static class stage2_Reducer extends Reducer<Text, Text, Text, IntWritable>{
		static IntWritable output =new IntWritable();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			int count =0;
			boolean tag = false;			
			for(Text val:values){			
				if(val.toString().equals("1"))
					tag=true;
				if(val.toString().equals("0"))
					count++;				
			}			
			if(tag==true){
				allNum += count;
			}	
			output.set(allNum);
		}
		
		protected void cleanup( Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			context.write(new Text("Triangle number"),output);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "Count Numbers");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(stage2.class);
	    job2.setMapperClass(stage2_Mapper.class);
	    job2.setReducerClass(stage2_Reducer.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job2, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    job2.waitForCompletion(true);
	}
}
