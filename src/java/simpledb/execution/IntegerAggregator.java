package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */


public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private Op agOp;
    private int gbIndex;
    private int agIndex;
    private TupleDesc td;

    private HashMap<Field, Integer> gbField2Val; //根据不同的Field聚合的不同结果
    private HashMap<Field, Integer[]> gbCountSum;


    public IntegerAggregator(int gbIndex, Type gbfieldtype, int agIndex, Op agOp, TupleDesc td) {
        // some code goes here
        this.gbIndex = gbIndex; // groupBy字段的索引
        this.gbfieldType = gbfieldtype; // groupBy 字段类型
        this.agIndex = agIndex; // 要被聚合的字段索引
        this.agOp = agOp; // 聚合操作符
        this.td = td; // 表描述

        gbField2Val = new HashMap<>(); // key: 分组字段，val:聚合结果
        gbCountSum = new HashMap<>(); // key: 分组字段，val: int[]{当前统计数目，当前统计总和}， AVG要用的
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
//    SELECT COUNT(ALL student_name) AS 总人数 FROM t_student GROUP BY (student_class);
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field aggreField; // 待聚合的字段，即student_name
        Field gbField; // groubBy what, 即根据学生班级分组，分组依据的字段就是 student class;
        Integer newVal; // 新的聚合结果， 即 count(all student_name)的结果
        aggreField = tup.getField(agIndex); // 获得待被聚合的元组的待聚合字段

        if(aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("该tuple不是指定的Int_Type类型");
        }

        int toAggregate = ((IntField)aggreField).getValue(); // 待聚合值，也就是答案的candidate之一

        if(gbIndex != Aggregator.NO_GROUPING) { // 判断是否分组
            gbField = tup.getField(gbIndex); // 如果分组，确定分组字段
        } else {
            gbField = null; // 不分组，则置为null
        }

//         其他聚合操作只需要和新的字段相比，平均值需要记录总和 单独处理
        if(agOp == Op.AVG) {
            avgAggregate(gbField, toAggregate);
            return;
        }

        // 如果已经分组聚合过
        if(gbField2Val.containsKey(gbField)) {
            Integer oldVal = gbField2Val.get(gbField);
            // 则考虑新元素后，重新计算聚合结果，对于sum，max, min, count 比较方便
            newVal = computeNewVal(oldVal, toAggregate, agOp);
        } else if(agOp == Op.COUNT) { //如果是第一次，并且是count
            newVal = 1;
        } else {
            newVal = toAggregate; // 非count从getValue开始
        }
        gbField2Val.put(gbField, newVal);
    }

    private void avgAggregate(Field gbField, int toAggregate) {
        if(gbCountSum.containsKey(gbField)) {
            // 从map取出旧值
            Integer[] oldCountSum = gbCountSum.get(gbField);
            Integer oldCount = oldCountSum[0];
            Integer oldSum = oldCountSum[1];
            // 更新为新值
            gbCountSum.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
        } else { //第一次处理这个分组
            // 对于AVG, 第一次应该存入，count：1， curVal: toAggregate
            gbCountSum.put(gbField, new Integer[]{1, toAggregate});
        }

        // 将答案存入【分组字段->聚合结果】
        Integer[] countAndSum = gbCountSum.get(gbField);
        Integer count = countAndSum[0];
        Integer sum = countAndSum[1];
        gbField2Val.put(gbField, sum / count);
        return;
    }

    private int computeNewVal(Integer oldVal, int toAggregate, Op agOp) {
        switch (agOp) {
            case COUNT:
                return oldVal + 1;
//            case AVG:
//                return (oldVal + toAggregate) / count; 不知道count是多少
            case MAX:
                return Math.max(oldVal, toAggregate);
            case MIN:
                return Math.min(oldVal, toAggregate);
            case SUM:;
                return oldVal + toAggregate;
            default:
                throw new IllegalArgumentException("聚合操作异常");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();

        for(Map.Entry<Field, Integer> filed2value : gbField2Val.entrySet()) {
            Tuple tuple = new Tuple(td);
            if(gbIndex == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(filed2value.getValue())); // 不分组，则新的tuple只有一列，为结果
            } else { // 分组，新的tuple为两列，第一列为分组id，第二列为聚合结果
                tuple.setField(0, filed2value.getKey());
                tuple.setField(1, new IntField(filed2value.getValue()));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}



