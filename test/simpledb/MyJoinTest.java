package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;

public class MyJoinTest {
    public static void main(String[] args) throws TransactionAbortedException, DbException {
        Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = {"filed0", "field1", "field2"};
        TupleDesc td = new TupleDesc(types, names);

        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1);

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2);

        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // Predicate, 在字段0中，找出比IntField类型 1 大的
        Predicate predicate = new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1));

        Filter sf1 = new Filter(predicate, ss1); // Filter 用断言在本表中过滤出有效字段

        // 对t1的字段1 和 t2的字段1 匹配相等的
        JoinPredicate joinPredicate = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join join = new Join(joinPredicate, sf1, ss2); // join t1已经过滤出的，和 t2整表

        join.open();
        while (join.hasNext()) {
            Tuple next = join.next();
            System.out.println(next);
        }
        join.close();
        Database.getBufferPool().transactionComplete(tid);
    }
}
