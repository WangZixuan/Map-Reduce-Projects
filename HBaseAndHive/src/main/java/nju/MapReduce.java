package nju;

/**
 * Created by Zixuan on 16-11-7.
 * Copy from last module of InvertedIndex.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce
{
    //static HBase hb=new HBase();

    static final String ROWNAME = "Wuxia_row";

    static List<Put> putList = new ArrayList<>();

    /**
     * Class for Map.
     */
    private static class InvertedIndexMap extends Mapper<Object, Text, Text, Text>
    {

        Text valueInfo = new Text();
        Text keyInfo = new Text();

        /**
         * Map.
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {

            //获取<key value>对所属的FileSplit对象
            FileSplit split = (FileSplit) context.getInputSplit();
            String filePath = split.getPath().toString().toLowerCase();

            StringTokenizer stk = new StringTokenizer(value.toString());
            while (stk.hasMoreElements())
            {
                //key值由（单词：URI）组成
                keyInfo.set(stk.nextToken() + ":" + filePath);
                //词频
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);

            }


        }
    }

    /**
     * Class for Combine.
     */
    private static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
    {

        Text valueInfo = new Text();
        Text keyInfo = new Text();

        /**
         * Combine, use reduce.
         * @param key
         * @param values
         * @param contex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context contex)
                throws IOException, InterruptedException
        {
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

    /**
     * Class for Reduce.
     */
    private static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text>
    {

        Text result = new Text();

        /**
         * Reduce.
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {

            //生成文档列表
            String fileList = new String();

            int sum = 0;
            int file_count = 0;
            for (Text value : values)
            {
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
            for (String snippet : set)
                resultStr.append(snippet).append(";");

            result.set(resultStr.substring(0, resultStr.length() - 1));

            //Write average results to result.
            double average = (double) sum / file_count;
            context.write(key, result);

            //Write to putLiit.
            Put put = new Put(Bytes.toBytes(ROWNAME));
            put.addColumn(Bytes.toBytes("Word"), Bytes.toBytes("Word"), Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("Count"), Bytes.toBytes("AverageCount"), Bytes.toBytes(average));
            putList.add(put);

            //Deprecated
            //hb.insertDataToTable(key.toString(), String.valueOf(average));
        }

        /**
         *
         * @param context
         * @throws IOException
         */
        @Override
        protected void cleanup(Context context) throws IOException
        {
            //hb.insertDataListToTable(putList);
            //hb.writeToFile();
            //hb.cleanup();
        }
    }

    public void MapReduceJob(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "InvertedIndex");

        job.setJarByClass(MapReduce.class);

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
