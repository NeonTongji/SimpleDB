package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbIdx;
    private Type gbFieldType;
    private int agIdx;
    private Op agOp;
    private TupleDesc td;
    HashMap<Field, Integer> agCache;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op agOp, TupleDesc td) {
        // some code goes here
        if(agOp != Op.COUNT) {
            throw new UnsupportedOperationException("String聚合只支持count, 不支持" + agOp);
        }
        this.gbIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.agIdx = afield;
        this.agOp = agOp;
        this.td = td;
        agCache = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // corner cases
        Field agField = tup.getField(agIdx);

        if(agField.getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("聚合类型应为字符串");
        }

        Field gbField;
        if(gbIdx == Aggregator.NO_GROUPING) {
            gbField = null;
        } else {
            gbField = tup.getField(gbIdx);
        }

        String toAggregate = ((StringField) tup.getField(agIdx)).getValue();
        // String只有count

        if(agCache.containsKey(gbField)) {
            int oldVal  = agCache.get(gbField);
            agCache.put(gbField, oldVal + 1);
        } else {
            agCache.put(gbField, 1);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        LinkedList<Tuple> tuples = new LinkedList<>();
        for(Map.Entry<Field, Integer> ag : agCache.entrySet()) {
            Tuple tuple = new Tuple(td);
            if(gbIdx == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(ag.getValue()));
            } else {
                tuple.setField(0, ag.getKey());
                tuple.setField(1,  new IntField(ag.getValue()));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

}
