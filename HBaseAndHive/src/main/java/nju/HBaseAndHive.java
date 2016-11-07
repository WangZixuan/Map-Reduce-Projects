package nju;
/**
 * For exp3.
 * HBase & Hive.
 * Created by Zixuan on 16-11-7.
 */


public class HBaseAndHive
{
    public static void main( String[] args )
    {
        MapReduce mr=new MapReduce();
        try
        {
            mr.MapReduceJob(args);
        }
        catch (java.io.IOException ioe)
        {

        }
        catch (java.lang.InterruptedException iee)
        {

        }
        catch (java.lang.ClassNotFoundException cnfe)
        {

        }
    }
}
