package nju;


import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;

/**
 * For exp3.
 * HBase & Hive.
 * Created by Zixuan on 16-11-7.
 */


public class HBaseAndHive
{
    /**
     * Main.
     * @param args
     */
    public static void main(String[] args)
    {
        PropertyConfigurator.configure("log4j.properties");

        MapReduce mr = new MapReduce();
        try
        {
            mr.MapReduceJob(args);

        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
