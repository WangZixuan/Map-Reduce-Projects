package nju;

/**
 * HBase Operations.
 * Created by Zixuan on 16-11-8.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

class HBase
{
    private final String TABLENAME = "Wuxia";
    private Configuration hbConfig;

    /**
     * Constructor.
     *
     * @throws IOException HBase needs it.
     */
    HBase() throws IOException
    {
        hbConfig = HBaseConfiguration.create();

        //Create table.
        HBaseAdmin admin = new HBaseAdmin(hbConfig);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLENAME));

        tableDescriptor.addFamily(new HColumnDescriptor("Word"));
        tableDescriptor.addFamily(new HColumnDescriptor("AverageCount"));

        if (admin.tableExists(tableDescriptor.getTableName()))
        {
            admin.disableTable(tableDescriptor.getTableName());
            admin.deleteTable(tableDescriptor.getTableName());
        }

        admin.createTable(tableDescriptor);
    }


    /**
     * Insert columns of data.
     *
     * @param putList A Put list to be put in the HBase.
     * @throws IOException HBase needs it.
     */
    void insertDataListToTable(List<Put> putList) throws IOException
    {
        HTable hTable=new HTable(hbConfig, TABLENAME);
        hTable.put(putList);
    }
    /**
     * Insert columns of data.
     *
     * @param put A Put class to be put in the HBase.
     * @throws IOException HBase needs it.
     */
    void insertDataToTable(Put put) throws IOException
    {
        HTable hTable=new HTable(hbConfig, TABLENAME);
        hTable.put(put);
    }

    /**
     * Close database.
     *
     * @throws IOException HBase needs it.
     */
    void cleanup() throws IOException
    {
        HTable hTable=new HTable(hbConfig, TABLENAME);
        hTable.close();
    }

    /**
     * Write the table to local file.
     *
     * @throws IOException HBase needs it.
     */
    void writeToFile() throws IOException
    {
        HTable hTable=new HTable(hbConfig, TABLENAME);
        ResultScanner rs = hTable.getScanner(new Scan());

        File averageCountFile = new File("AverageCount.txt");
        if (!averageCountFile.exists())
            averageCountFile.createNewFile();
        else
        {
            averageCountFile.delete();
            averageCountFile.createNewFile();
        }

        PrintWriter writer = new PrintWriter("AverageCount.txt", "UTF-8");
        for (Result r : rs)
            for (KeyValue keyValue : r.raw())
                writer.println(keyValue.getFamily().toString() + keyValue.getKeyString());

        writer.close();
    }
}