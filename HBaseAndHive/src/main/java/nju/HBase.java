package nju;

/**
 * Created by Zixuan on 16-11-8.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * HBase Operations.
 */
public class HBase
{
    HTable hTable;
    Configuration hbConfig = HBaseConfiguration.create();

    final String TABLENAME = "Wuxia";

    /**
     * Constructor.
     */
    public HBase()
    {

        //Create table.
        try
        {
            Connection connection = ConnectionFactory.createConnection(hbConfig);
            Admin admin = connection.getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLENAME));

            tableDescriptor.addFamily(new HColumnDescriptor("Word"));
            tableDescriptor.addFamily(new HColumnDescriptor("AverageCount"));

            if (admin.tableExists(tableDescriptor.getTableName()))
            {
                admin.disableTable(tableDescriptor.getTableName());
                admin.deleteTable(tableDescriptor.getTableName());
            }

            admin.createTable(tableDescriptor);

            connection.close();

        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }


    /**
     * Insert columns of data.
     * @param putList
     * @throws IOException
     */
    public void insertDataListToTable(List<Put> putList) throws IOException
    {
        hTable.put(putList);
    }

    /**
     * Close database.
     */
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
     * Write the table to local file.
     */
    public void writeToFile() throws IOException
    {
        ResultScanner rs = hTable.getScanner(new Scan());

        File averageCountFile = new File("AverageCount.txt");
        if (!averageCountFile.exists())
            averageCountFile.createNewFile();
        else
            throw new FileAlreadyExistsException("AverageCount.txt");

        PrintWriter writer = new PrintWriter("AverageCount.txt", "UTF-8");
        for (Result r : rs)
            for (KeyValue keyValue : r.raw())
                writer.println(keyValue.getFamily().toString() + keyValue.getKeyString());

        writer.close();
    }
}