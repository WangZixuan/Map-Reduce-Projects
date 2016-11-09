package nju;

import org.apache.log4j.PropertyConfigurator;

/**
 * For exp3.
 * HBase & Hive.
 * Created by Zixuan on 16-11-7.
 */
public class HBaseAndHive
{
    /**
     * Main.
     *
     * @param args Args passed from input.
     */
    public static void main(String[] args)
    {
        PropertyConfigurator.configure("log4j.properties");

        try
        {
            MapReduce mr = new MapReduce();
            mr.MapReduceJob(args);

        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
