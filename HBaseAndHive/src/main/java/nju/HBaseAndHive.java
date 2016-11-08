package nju;


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
        MapReduce mr = new MapReduce();
        try
        {
            File averageCountFile = new File("AverageCount.txt");
            if (!averageCountFile.exists())
                averageCountFile.createNewFile();
            else
                throw new FileAlreadyExistsException("AverageCount.txt");

            mr.MapReduceJob(args);

        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
