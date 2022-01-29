//package simpledb.execution;
//
//import simpledb.common.Database;
//import simpledb.storage.DbFileIterator;
//import simpledb.storage.TupleIterator;
//import simpledb.transaction.TransactionAbortedException;
//import simpledb.common.DbException;
//import simpledb.storage.Tuple;
//import simpledb.storage.TupleDesc;
//
//import java.util.*;
//
///**
// * Filter is an operator that implements a relational select.
// */
//public class Filter extends Operator {
//
////    @Override
////    public String getName() {
////        return "<Filter-" + p.toString() + "on" + ">";
////    }
//
//    private static final long serialVersionUID = 1L;
//    private Predicate p;
//    private OpIterator iterator; //OpIterator 是所有运算符的公共接口, 只有这个迭代器开启，运算符方法才生效
//    private TupleIterator filterResult; // Filter过滤结果，通过其元组迭代器获取
//    private TupleDesc td;
//    /**
//     * Constructor accepts a predicate to apply and a child operator to read
//     * tuples to filter from.
//     *
//     * @param p
//     *            The predicate to filter tuples with
//     * @param child
//     *            The child operator
//     */
//    public Filter(Predicate p, OpIterator child) {
//        // some code goes here
//        this.p = p;
//        this.iterator = child;
//        this.td = child.getTupleDesc();
//    }
//
//    public Predicate getPredicate() {
//        // some code goes here
//        return p;
//    }
//
//    public TupleDesc getTupleDesc() {
//        // done
//        return td;
//    }
//
//    public void open() throws DbException, NoSuchElementException,
//            TransactionAbortedException {
//        // some code goes here
//        iterator.open();
//        super.open();
//        // 按照构造器的迭代器和断言，返回过滤结果
//        filterResult = filter(iterator, p);
//        // 开启过滤结果迭代
//        filterResult.open();
//    }
//
//    public void close() {
//        // some code goes here
//        filterResult = null; // 过滤结果迭代器
//        iterator.close(); // 所有元组的迭代器
//        super.close();
//
//    }
//
//    public void rewind() throws DbException, TransactionAbortedException {
//        // some code goes here
//        // 将过滤结果重置
//        filterResult.rewind();
//    }
//
//    /**
//     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
//     * child operator, applying the predicate to them and returning those that
//     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
//     * 不断迭代，直到找到满足断言predicate的元组
//     *
//     * @return The next tuple that passes the filter, or null if there are no
//     *         more tuples
//     * @see Predicate#filter
//     */
//    protected Tuple fetchNext() throws NoSuchElementException,
//            TransactionAbortedException, DbException {
//        // done
//        // 不断迭代，直到找到满足断言predicate的元组
//        if(filterResult.hasNext()) {
//            return filterResult.next();
//        }
//        return null;
//
//    }
//
//    private TupleIterator filter(OpIterator iter, Predicate predicate) throws TransactionAbortedException, DbException {
//        List<Tuple> tuples = new ArrayList<>();
//
//        while(iter.hasNext()) {
//            Tuple t = iter.next();
//            if(predicate.filter(t)) tuples.add(t);
//        }
//
//        return new TupleIterator(getTupleDesc(), tuples);
//    }
//
//    /**
//     * @return return the children DbIterators of this operator. If there is
//     *         only one child, return an array of only one element. For join
//     *         operators, the order of the children is not important. But they
//     *         should be consistent among multiple calls.
//     * */
//    @Override
//    public OpIterator[] getChildren() {
//        // some code goes here
//        return new OpIterator[]{this.iterator};
//    }
//
//    /**
//     * Set the children(child) of this operator. If the operator has only one
//     * child, children[0] should be used. If the operator is a join, children[0]
//     * and children[1] should be used.
//     *
//     *
//     * @param children
//     *            the DbIterators which are to be set as the children(child) of
//     *            this operator
//     * */
//    @Override
//    public void setChildren(OpIterator[] children) {
//        // some code goes here
//        this.iterator = children[0];
//    }
//
//}

package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    // TODO: 17-7-15 觉得碍眼可以删了，可以帮助了解sql执行过程的每一步
//    @Override
//    public String getName() {
//        return "<Filter-" + predicate.toString() + " on " + child.getName() + ">";
//    }

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    private TupleDesc td;

    private OpIterator child;

    //缓存过滤结果，加快hasNext和next方法
    private TupleIterator filterResult;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate=p;
        this.child=child;
        this.td=child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        filterResult = filter(child, predicate);
        filterResult.open();
    }

    private TupleIterator filter(OpIterator child, Predicate predicate) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (predicate.filter(t)) {
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        filterResult = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        filterResult.rewind();
    }



    /**
     * AbstractOpIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(filterResult.hasNext())
            return filterResult.next();
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
