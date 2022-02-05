package simpledb.execution;

import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc td;
    private TupleIterator joinResultsIter;
    private int numFields;

    public static int blockMemory = (1 << 19);


    /** 131072 bytes （1 << 17）是BlockNestedLoopJoin算法中默认缓冲区大小
     * 增大这个参数可以减小磁盘的io次数，测试后 131072 大小缓冲区IO耗时
     * 1倍：25s
     * 2倍：15s
     * 5倍 10s
     * 10倍 6s
      */


//    private int blockMemory = (1 << 17) * 4;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // done
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.numFields = child1.getTupleDesc().numFields();
        this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();
        joinResultsIter = getJoinResult();
        joinResultsIter.open();
    }

    private TupleIterator getJoinResult() throws TransactionAbortedException, DbException {
        LinkedList<Tuple> joinedTuples = new LinkedList<>();

        int blockSize1 = blockMemory / child1.getTupleDesc().getSize();
        int blockSize2 = blockMemory / child2.getTupleDesc().getSize();

        Tuple[] leftBlockTuples = new Tuple[blockSize1]; // 缓冲区可以缓冲child1的元组数
        Tuple[] rightBlockTuples = new Tuple[blockSize2]; // 缓冲区可以缓冲child2的元组数

        int idx1 = 0;
        int idx2 = 0;


        child1.rewind();// child1清零
        while (child1.hasNext()) {
            Tuple leftTuple = child1.next();
            // 放入左表缓冲区
            leftBlockTuples[idx1++] = leftTuple;
            // 左表缓冲区满了
            if(idx1 >= blockSize1) {
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple rightTuple = child2.next();
                    // 放入右表缓冲区
                    rightBlockTuples[idx2++] = rightTuple;
                    // 此时左右都满了，便可以join一次
                    if(idx2 >= blockSize2) {
                        // 将左右缓冲区的元组整理到tuples中
                        sortedJoin(joinedTuples, leftBlockTuples, rightBlockTuples);
                        // 清空右侧缓冲区
                        Arrays.fill(rightBlockTuples, null);
                        idx2 = 0;
                    }
                }
                // 处理右边缓冲区最后一批余数
                if(idx2 > 0 && idx2 < rightBlockTuples.length) {
                    sortedJoin(joinedTuples, leftBlockTuples, rightBlockTuples);
                    // 清空右侧缓冲区
                    Arrays.fill(rightBlockTuples, null);
                    idx2 = 0;
                }

                // 缓冲区遍历完一次右表 就要又把左缓冲区重置
                Arrays.fill(leftBlockTuples, null);
                idx1 = 0;
            }
        }

        // 处理左表缓冲区中剩下的tuples
        if(idx1 > 0 && idx1 < blockSize1) {
            child2.rewind();
            while (child2.hasNext()) {
                Tuple rightTuple = child2.next();
                rightBlockTuples[idx2++] = rightTuple;
                // 此时左右都满了，便可以join一次
                if(idx2 >= blockSize2) {
                    sortedJoin(joinedTuples, leftBlockTuples, rightBlockTuples);
                    // 清空右侧缓冲区
                    Arrays.fill(rightBlockTuples, null);
                    idx2 = 0;
                }
            }
            // 处理最后一批余数
            if(idx2 > 0 && idx2 < rightBlockTuples.length) {
                sortedJoin(joinedTuples, leftBlockTuples, rightBlockTuples);
                // 清空右侧缓冲区
                Arrays.fill(rightBlockTuples, null);
                idx2 = 0;
            }

        }

        return new TupleIterator(getTupleDesc(), joinedTuples); // 目标变为求join的tuples
    }

    private void sortedJoin(LinkedList<Tuple> tuples, Tuple[] lcb, Tuple[] rcb) {

        int m = lcb.length - 1, n = rcb.length - 1;
        while(m > 0 && lcb[m] == null) m--; // 注意这里要去重，不然无法和null值比较
        while(n > 0 && rcb[n] == null) n--;

        Tuple[] leftBlockTuples = new Tuple[m + 1];
        Tuple[] rightBlockTuples = new Tuple[n + 1];
        System.arraycopy(lcb, 0, leftBlockTuples, 0, m + 1);
        System.arraycopy(rcb, 0, rightBlockTuples, 0, n + 1);

        int index1 = joinPredicate.getField1();
        int index2 = joinPredicate.getField2();

        // 自己比较，用来排序
        JoinPredicate eqPredicate1 = new JoinPredicate(index1, Predicate.Op.EQUALS, index1);
        JoinPredicate ltPredicate1 = new JoinPredicate(index1, Predicate.Op.LESS_THAN, index1);
        JoinPredicate gtPredicate1 = new JoinPredicate(index1, Predicate.Op.GREATER_THAN, index1);


        // 互相比较，用来断言
        JoinPredicate eqPredicate2 = new JoinPredicate(index1, Predicate.Op.EQUALS, index2);
        JoinPredicate ltPredicate2 = new JoinPredicate(index1, Predicate.Op.LESS_THAN, index2);
        JoinPredicate gtPredicate2 = new JoinPredicate(index1, Predicate.Op.GREATER_THAN, index2);


        Comparator<Tuple> comparator = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if(ltPredicate1.filter(o1, o2)) return -1;
                if(gtPredicate1.filter(o1, o2)) return 1;
                return 0;
            }
        };

        Arrays.sort(leftBlockTuples, comparator);
        Arrays.sort(rightBlockTuples, comparator);

        int pos1, pos2;

        switch (joinPredicate.getOperator()) {

            // 如果运算符为相等，则把相等的tuple加入tuples返回
            case EQUALS:

                eqJoin(tuples, leftBlockTuples, rightBlockTuples, eqPredicate1, eqPredicate2, ltPredicate2);

                break;

            case LESS_THAN:
                // 同下

            case LESS_THAN_OR_EQ:

                ltJoin(tuples, leftBlockTuples, rightBlockTuples);
                break;

            case GREATER_THAN:
                // 同下

            case GREATER_THAN_OR_EQ:
                gtJoin(tuples, leftBlockTuples, rightBlockTuples);

                break;

            default:
                throw new RuntimeException("JoinPredicate非法");
        }

    }

    /**
     * 使得左右缓冲区类，断言匹配的元组，join后加入tuples
     * 应该采用双指针合并数组,
     * 如果断言匹配，查找是否有重复的，有重复的指针都前移动，然后把这段全部归并
     * 当大小不相等后，利用双指针中小的那个前移
     * @param tuples
     * @param leftBlockTuples
     * @param rightBlockTuples
     * @param eqPredicate1
     * @param eqPredicate2
     * @param ltPredicate2
     */
    private void eqJoin(LinkedList<Tuple> tuples, Tuple[] leftBlockTuples, Tuple[] rightBlockTuples, JoinPredicate eqPredicate1, JoinPredicate eqPredicate2, JoinPredicate ltPredicate2) {
        int pos2;
        int pos1;
        pos1 = pos2 = 0;
        while(pos1 < leftBlockTuples.length && pos2 < rightBlockTuples.length) {
            Tuple leftTuple = leftBlockTuples[pos1];
            Tuple rightTuple = rightBlockTuples[pos2];
            // 如果断言吻合，则将左右元组merge
            if(eqPredicate2.filter(leftTuple, rightTuple)) {
                // 记录好相同的起点
                int start1 = pos1, start2 = pos2;
                // 相同的
                while (pos1 < leftBlockTuples.length
                        && eqPredicate1.filter(leftTuple, leftBlockTuples[pos1])) {
                    pos1++;
                };
                while (pos2 < rightBlockTuples.length
                        && eqPredicate1.filter(rightTuple, rightBlockTuples[pos2])) {
                    pos2++;
                };
                // 相同终点（不包含，为下一个）
                int end1 = pos1, end2 = pos2;

                //把这段相同的加入交集
                for (int i = start1; i < end1; i++) {
                    for(int j = start2; j < end2; j++) {
                        Tuple mergeTuples = mergeTuples(leftBlockTuples[i], rightBlockTuples[j]);
                        tuples.add(mergeTuples);
                    }
                }
            } else if(ltPredicate2.filter(leftTuple, rightTuple)) {
                pos1++;
            } else {
                pos2++;
            }
        }
    }

    private void ltJoin(LinkedList<Tuple> tuples, Tuple[] leftBlockTuples, Tuple[] rightBlockTuples) {
        int pos2;
        int pos1;
        for(pos1 = 0; pos1 < leftBlockTuples.length; pos1++) {
            Tuple leftTuple = leftBlockTuples[pos1];
            for(pos2 = rightBlockTuples.length - 1; pos2 >= 0; pos2--) {
                Tuple rightTuple = rightBlockTuples[pos2];
                if(joinPredicate.filter(leftTuple, rightTuple)) {
                    Tuple result = mergeTuples(leftTuple, rightTuple);
                    tuples.add(result);
                } else {
                    break;
                }
            }
        }
    }

    private void gtJoin(LinkedList<Tuple> tuples, Tuple[] leftBlockTuples, Tuple[] rightBlockTuples) {

        for(int pos1 = leftBlockTuples.length - 1; pos1 >= 0; pos1--) {
            Tuple leftTuple = leftBlockTuples[pos1];
            for(int pos2 = 0; pos2 < rightBlockTuples.length; pos2++) {
                Tuple rightTuple = rightBlockTuples[pos2];
                if(joinPredicate.filter(leftTuple, rightTuple)) {
                    Tuple result = mergeTuples(leftTuple, rightTuple);
                    tuples.add(result);
                } else {
                    break;
                }
            }
        }
    }






    // 两个数组合并，但是rightTuple起点要偏移整个left长度
    private Tuple mergeTuples(Tuple leftTuple, Tuple rightTuple) {
        Tuple tuple = new Tuple(td);
        for (int i = 0; i < numFields; i++) {
            tuple.setField(i, leftTuple.getField(i));
        }

        for (int i = 0; i < child2.getTupleDesc().numFields(); i++) {
            tuple.setField(numFields + i, rightTuple.getField(i)) ;
        }

        return tuple;
    }


    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        joinResultsIter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        joinResultsIter.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(joinResultsIter.hasNext()) {
            return joinResultsIter.next();
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}


