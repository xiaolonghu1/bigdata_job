package com.geekbang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBase {
    public static void main(String[] args) throws IOException {

        //Get Connection
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        //Create Table
        TableName tableName = TableName.valueOf("huxiaolong:student");
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor familyInfo = new HColumnDescriptor("info");
            HColumnDescriptor familyScore = new HColumnDescriptor("score");
            hTableDescriptor.addFamily(familyInfo);
            hTableDescriptor.addFamily(familyScore);
            try {
                admin.createTable(hTableDescriptor);
            } catch (NamespaceNotFoundException e) {
                //If namespace not found, create it.
                admin.createNamespace(NamespaceDescriptor.create("huxiaolong").build());
                admin.createTable(hTableDescriptor);
            }
            System.out.println("Table create successful");
        }

        //Insert
        Put put = new Put(Bytes.toBytes("Tom"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        conn.getTable(tableName).put(put);

        //TODO:Others
        Put put1 = new Put(Bytes.toBytes("Hu Xiaolong"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20220735030074"));
        conn.getTable(tableName).put(put1);

        //Query
        Get get = new Get(Bytes.toBytes("Hu Xiaolong"));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // Delete Record
        Delete delete = new Delete(Bytes.toBytes("Hu Xiaolong"));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // Delete table
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
