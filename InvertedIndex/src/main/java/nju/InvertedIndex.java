package nju;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
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

    private static class InvertedIndexMap extends Mapper<Object, Text, Text, Text> {

        Text valueInfo = new Text();
        Text keyInfo = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //获取<key value>对所属的FileSplit对象
            FileSplit split = (FileSplit) context.getInputSplit();
            String filePath=split.getPath().toString().toLowerCase();

            StringTokenizer stk = new StringTokenizer(value.toString());
            while (stk.hasMoreElements()) {
                //key值由（单词：URI）组成
                keyInfo.set(stk.nextToken() + ":" + filePath);
                //词频
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);

            }


        }
    }

    private static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        Text valueInfo = new Text();
        Text keyInfo = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context contex)
                throws IOException, InterruptedException {


            int sum = 0;
            for (Text value : values)
                sum += Integer.parseInt(value.toString());

            int splitIndex = key.toString().indexOf(":");
            //重新设置value值由（URI:词频)组成
            valueInfo.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            //重新设置key值为单词
            keyInfo.set(key.toString().substring(0, splitIndex));
            contex.write(keyInfo, valueInfo);
        }
    }

    private static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {

        Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //生成文档列表
            String fileList = new String();
            String average = new String();

            int sum = 0;
            int file_count = 0;
            for (Text value : values) {
                file_count++;
                String s = value.toString();
                fileList += s.substring(s.lastIndexOf("/") + 1, s.indexOf(".txt")) + s.substring(s.lastIndexOf(":")) + ";";
                sum += Integer.parseInt(s.substring(s.lastIndexOf(":") + 1));
            }

            //Sort fileList.
            String[] sp = fileList.split(";");

            Set<String> set = new TreeSet<String>();

            for (int i = 0; i < sp.length; ++i)
                set.add(sp[i]);

            StringBuilder resultStr = new StringBuilder();
            for (String snipet : set)
                resultStr.append(snipet).append(";");

            average += String.valueOf((double)sum / file_count);
            result.set(average + "," + resultStr.substring(0, resultStr.length()-1));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        PropertyConfigurator.configure("log4j.properties");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "InvertedIndex");

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
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
