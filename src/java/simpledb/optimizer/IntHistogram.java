package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int width;
    private int ntups;
    private int[] histogram;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        double range = (double) (max - min + 1) / buckets;
        width = (int) Math.ceil(range);
        this.ntups = 0;
        this.histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = valueToIdx(v);
        histogram[idx]++;
        ntups++;
    }

    // 通过v的值，锁定在哪个桶里
    private int valueToIdx(int v) {
         if(v == max) {
             return buckets - 1;
         } else {
             return (v - min) / width;
         }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        int bucketIdx = valueToIdx(v);
        int height = 0; // height为直方图高度，即落在这个桶的数据数量
        int right = bucketIdx * width + min + width - 1; // 桶的右边界索引
        int left = bucketIdx * width + min; // 桶的左边界索引

        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0.0;
                } else {
                    height = histogram[bucketIdx];
                    return (double) height / width / ntups;
                }

            case GREATER_THAN:
                if (v < min) {
                    return 1.0;
                } else if (v > max) {
                    return 0.0;
                } else {
                    height = histogram[bucketIdx];
                    // p1: 这个桶范围内大于v的概率
                    double p1 = ((right - v) / width * 1.0) * (height * 1.0 / ntups);
                    int allAtRight = 0;
                    for (int i = bucketIdx + 1; i < buckets; i++) {
                        allAtRight += histogram[i];
                    }
                    // p2: 所有这个桶右侧的数据出现概率
                    double p2 = allAtRight * 1.0 / ntups;
                    return p1 + p2;
                }

            case LESS_THAN:
                if (v < min) {
                    return 0.0;
                } else if (v > max) {
                    return 1.0;
                } else {
                    height = histogram[bucketIdx];
                    double p1 = ((v - left) / width * 1.0) * (height * 1.0 / ntups);
                    int allAtLeft = 0;
                    for (int i = bucketIdx - 1; i >= 0; i--) {
                        allAtLeft += histogram[i];
                    }
                    double p2 = allAtLeft * 1.0 / ntups;
                    return p1 + p2;
                }

            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.LESS_THAN, v);

            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.GREATER_THAN, v);

            case LIKE:
                //String才支持like
                return avgSelectivity();

            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);

            default:
                throw new RuntimeException("Op错误");
        }
    }


    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity(){
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
