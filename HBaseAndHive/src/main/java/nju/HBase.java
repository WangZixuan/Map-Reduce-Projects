package nju;

/**
 * HBase Operations.
 * Created by Zixuan on 16-11-8.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

class HBase
{
    private HTable hTable;

    private final String TABLENAME = "Wuxia";

    /**
     * Constructor.
     *
     * @throws IOException HBase needs it.
     */
    HBase() throws IOException
    {
        Configuration hbConfig = HBaseConfiguration.create();
        hTable = new HTable(hbConfig, TABLENAME);

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
        hTable.put(put);
    }

    /**
     * Close database.
     *
     * @throws IOException HBase needs it.
     */
    void cleanup() throws IOException
    {
        hTable.close();
    }

    /**
     * Write the table to local file.
     *
     * @throws IOException HBase needs it.
     */
    void writeToFile() throws IOException
    {
        //Create a file.
        File averageCountFile = new File("AverageCount.txt");
        if (averageCountFile.exists())
            averageCountFile.delete();

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("AverageCount"), Bytes.toBytes("AverageCount"));
        ResultScanner rs = hTable.getScanner(scan);

        PrintWriter writer = new PrintWriter("AverageCount.txt", "UTF-8");

        for (Result r : rs)
        {
            writer.print(new String(r.getRow()) + "\t");
            for (KeyValue keyValue : r.raw())
                writer.print(new String(keyValue.getValue()));
            writer.println();
        }
        writer.close();
    }
}