package nju;

/**
 * Created by Zixuan on 16-11-8.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase Operations.
 */
public class HBase
{
    Configuration hbConfig;
    HTable hTable;

    final String TABLENAME = "Wuxia";
    final String ROWNAME = "Wuxia_row";

    /**
     * Constructor.
     */
    public HBase()
    {
        hbConfig = HBaseConfiguration.create();

        //Create table.
        try
        {
            hTable = new HTable(hbConfig, TABLENAME);
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    /**
     * Insert one column of data.
     *
     * @param word    inserted word
     * @param avCount inserted average word count
     * @throws IOException
     */
    public void insertDataToTable(String word, String avCount) throws IOException
    {
        Put put = new Put(Bytes.toBytes(ROWNAME));
        put.addColumn(Bytes.toBytes("Word"), Bytes.toBytes("Word"), Bytes.toBytes(word));
        put.addColumn(Bytes.toBytes("Count"), Bytes.toBytes("AverageCount"), Bytes.toBytes(avCount));
        hTable.put(put);
    }

    public void cleanup()
    {
        try
        {
            hTable.close();
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }

    }

    /**
     *
     */
    public void writeToFile()
    {

    }
}