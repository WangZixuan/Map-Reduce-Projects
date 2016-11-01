package nju;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;


public class InvertedIndex {

    public static class InvertedIndexMap extends Mapper<Object,Text,Text,Text>{

        private Text valueInfo = new Text();
        private Text keyInfo = new Text();
        private FileSplit split;

        public void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            //获取<key value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
            while (stk.hasMoreElements()) {
                //key值由（单词：URI）组成
                keyInfo.set(stk.nextToken()+":"+split.getPath().toString());
                //词频
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);

            }


        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{

        Text info = new Text();

        public void reduce(Text key, Iterable<Text> values,Context contex)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            int splitIndex = key.toString().indexOf(":");
            //重新设置value值由（URI+:词频组成）
            info.set(key.toString().substring(splitIndex+1) +":"+ sum);
            //重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));
            contex.write(key, info);
        }
    }

    public static class InvertedIndexReduce extends Reducer<Text,Text,Text,Text>{

        private Text result = new Text();
        private Text result_average = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //生成文档列表
            String fileList = new String();
            String average = new String();
            double sum=0;
            double file_count=0;
            for (Text value : values) {
                file_count++;
                fileList += value.toString()+";";
                String s=value.toString();
                sum+= Double.parseDouble(s.substring(s.lastIndexOf(":")+1));
            }
            average+=String.valueOf(sum/file_count);
            result.set(fileList);
            result_average.set(average);
            context.write(key, result);
            context.write(key, result_average);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        PropertyConfigurator.configure("log4j.properties");
        Configuration conf = new Configuration();

        Job job = new Job(conf,"InvertedIndex");

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(InvertedIndexCombiner.class);

        job.setReducerClass(InvertedIndexReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}

