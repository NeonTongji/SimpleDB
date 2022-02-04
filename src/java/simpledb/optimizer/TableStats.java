package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() throws TransactionAbortedException, DbException {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;
    private int ioCostPerPage;
    private TupleDesc td;
    private int ntups; // tups数量
    private HeapFile table;
    private HashMap<String, Integer[]> attrs; // key为每一列字段名，int[]{最小值，最大值}
    private HashMap<String, Object> name2hist;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) throws TransactionAbortedException, DbException {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.td = table.getTupleDesc();
        this.attrs = new HashMap<>();
        this.name2hist = new HashMap<>();
        Transaction t = new Transaction();
        DbFileIterator iterator = table.iterator(t.getId());
        process(iterator);
    }

    /**
     * 计算table中tuple的数量，计算每个int类型的最小值和最大值，计算每个字段的histogram
     * @param iterator
     */
    private void process(DbFileIterator iterator) throws TransactionAbortedException, DbException {
        iterator.open();
        // 统计tuple并统计字段的最值
        findMinMax(iterator);

        // 计算每一列的Histogram直方图
        makeHistogram(iterator);

    }

    private void makeHistogram(DbFileIterator iterator) throws DbException, TransactionAbortedException {
        for(Map.Entry<String, Integer[]> entry : attrs.entrySet()) {
            Integer[] value = entry.getValue();
            IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, value[0], value[1]);
            name2hist.put(entry.getKey(), intHistogram);
        }

        iterator.rewind(); // 重置迭代器，以建立直方图

        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();

            for(int i = 0; i < td.numFields(); i++) {
                Type fieldType = td.getFieldType(i);
                String fieldName = td.getFieldName(i);

                // int类型由于最值已经确定，每个桶都建立好了
                if(fieldType == Type.INT_TYPE) {
                    int value = ((IntField) tuple.getField(i)).getValue();
                    IntHistogram intHistogram  = (IntHistogram) name2hist.get(fieldName);
                    intHistogram.addValue(value);
                    name2hist.put(fieldName, intHistogram);
                }

                // String类型由于不确定分配到哪里，如果出现新值，应该新建直方图
                else if(fieldType == Type.STRING_TYPE) {
                    String value = ((StringField) tuple.getField(i)).getValue();
                    if(name2hist.containsKey(fieldName)) {
                        StringHistogram stringHistogram = (StringHistogram) name2hist.get(fieldName);
                        stringHistogram.addValue(value);
                        name2hist.put(fieldName, stringHistogram);
                    } else {
                        StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                        stringHistogram.addValue(value);
                        name2hist.put(fieldName,stringHistogram);
                    }
                }
            }
        }
    }

    private void findMinMax(DbFileIterator iterator) throws DbException, TransactionAbortedException {
        while (iterator.hasNext()) {
            ntups++; //记录tuple的数量
            Tuple tuple = iterator.next();
            for(int i = 0; i < td.numFields(); i++) {
                Type fieldType = td.getFieldType(i);
                // 只用处理int类型，因为string类型没有最大最小值
                if(fieldType == Type.INT_TYPE) {
                    String fieldName = td.getFieldName(i);
                    Integer value = ((IntField) tuple.getField(i)).getValue();
                    if(attrs.containsKey(fieldName)) {
                        if(value < attrs.get(fieldName)[0]) {
                            attrs.get(fieldName)[0] = value;
                        }
                        if(value > attrs.get(fieldName)[1]) {
                            attrs.get(fieldName)[1] = value;
                        }
                    } else {
                        attrs.put(fieldName, new Integer[]{value, value});
                    }
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    //返回预估的读取table所需时间，不考虑页不完整，不考虑缓存
    public double estimateScanCost() {
        // some code goes here
        return table.numPages() * ioCostPerPage; // 多少页就多少时间
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = td.getFieldType(field);
        String fieldName = td.getFieldName(field);
        if(fieldType == Type.INT_TYPE){
            int value = ((IntField) constant).getValue();
            IntHistogram intHistogram = (IntHistogram) name2hist.get(fieldName);
            return intHistogram.estimateSelectivity(op, value);
        } else {
            String value = ((StringField) constant).getValue();
            StringHistogram stringHistogram = (StringHistogram) name2hist.get(fieldName);
            return stringHistogram.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
