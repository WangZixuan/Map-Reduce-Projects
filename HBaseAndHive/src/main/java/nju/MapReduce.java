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

class MapReduce
{
    private static HBase hb;
    private static List<Put> putList;

    /**
     * Constructor.
     *
     * @throws IOException HBase needs it.
     */
    MapReduce() throws IOException
    {
        hb = new HBase();
        putList = new ArrayList<>();
    }

    /**
     * Class for Map.
     */
    private static class InvertedIndexMap extends Mapper<Object, Text, Text, Text>
    {

        Text valueInfo = new Text();
        Text keyInfo = new Text();

        /**
         * Map.
         *
         * @param key     Input key.
         * @param value   Input value.
         * @param context Output.
         * @throws IOException          Map needs it.
         * @throws InterruptedException Map needs it.
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {

            //Get <key value> from FileSplit.
            FileSplit split = (FileSplit) context.getInputSplit();
            String filePath = split.getPath().toString().toLowerCase();

            StringTokenizer stk = new StringTokenizer(value.toString());
            while (stk.hasMoreElements())
            {
                //key:(word：URI).
                keyInfo.set(stk.nextToken() + ":" + filePath);
                //Count of the word.
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
         *
         * @param key     Input key.
         * @param values  Input values.
         * @param context Output.
         * @throws IOException          Combine needs it.
         * @throws InterruptedException Combine needs it.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (Text value : values)
                sum += Integer.parseInt(value.toString());

            int splitIndex = key.toString().indexOf(":");
            //Reset value to（URI:count).
            valueInfo.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            //Reset key to word.
            keyInfo.set(key.toString().substring(0, splitIndex));
            context.write(keyInfo, valueInfo);
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
         *
         * @param key     Input key.
         * @param values  Input values.
         * @param context Output.
         * @throws IOException          Reduce needs it.
         * @throws InterruptedException Reduce needs it.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {

            //Generate list of docs.
            String fileList = "";

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

            Set<String> set = new TreeSet<>();

            Collections.addAll(set, sp);

            StringBuilder resultStr = new StringBuilder();
            for (String snippet : set)
                resultStr.append(snippet).append(";");

            //Write inverted indexes.
            result.set(resultStr.substring(0, resultStr.length() - 1));
            context.write(key, result);

            double average = (double) sum / file_count;
            //Write to putList.
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("AverageCount"), Bytes.toBytes("AverageCount"), Bytes.toBytes(String.valueOf(average)));
            putList.add(put);

            //Deprecated.
            //hb.insertDataToTable(put);
        }

    }

    /**
     * Do the MapReduce job.
     *
     * @param args args for input and output.
     * @throws IOException            MapReduce needs it.
     * @throws InterruptedException   MapReduce needs it.
     * @throws ClassNotFoundException MapReduce needs it.
     */
    void MapReduceJob(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "MapReduceHBase");

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


        if (job.waitForCompletion(true))
        {
            hb.insertDataListToTable(putList);
            hb.writeToFile();
            hb.cleanup();
        }

    }
}