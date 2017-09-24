package org.bigdata.myhadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author liuhui
 * @version V1.0
 * @Package org.bigdata.myhadoop.hbase
 * @Description: this is java class
 * @date 2017/9/20 23:07
 */
public class HBaseOperation {


    public static HTable getHTableByTableName(String tableName) throws Exception {

        //get Configuration

        Configuration configuration = HBaseConfiguration.create();
        //get table
        HTable table = new HTable(configuration, tableName);

        return table;
    }


    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String tableName = "blog";

        HTable table = getHTableByTableName(tableName);

        //Create Get
        Get get = new Get(Bytes.toBytes("blog3"));

        table.get(get);

    }

}
