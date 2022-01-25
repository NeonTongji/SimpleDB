package simpledb;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.*;

public class test {

    public static void main(String[] argv) {

        // construct a 3-column table schema
        // 第一步，创建一个三列的表格，多少个元组还不清楚
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        // 第二步 根据描述器和文件，创建一个表
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);

        // 第三步 把表加到目录中，并且跟他给一个名字，如 test
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        // 第四步，为本次查询声明一个事务id，基于事务进行select
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        // 第五步，利用迭代器，迭代元组
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}