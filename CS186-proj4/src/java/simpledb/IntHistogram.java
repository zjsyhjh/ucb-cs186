package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
    /* my code for proj3 */
    private int min;
    private int max;
    private int buckets;
    private int bucketSize;
    private int totalElement;
    private HashMap<Integer, Integer> bucketMap;
    /* my code for proj3 */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        /* my code for proj3 */
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.totalElement = 0;
        this.bucketSize = (max - min) / buckets + 1;
        bucketMap = new HashMap<Integer, Integer>();
        for (int i = 1; i <= buckets; i++) {
            bucketMap.put(i, 0);
        }
        /* my code for proj3 */
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        /* my code for proj3 */
        int pos = (v - min) / bucketSize + 1;
        bucketMap.put(pos, bucketMap.get(pos) + 1);
        totalElement += 1;
        /* my code for proj3 */
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
        // return -1.0;
        /* my code for proj3 */
        if (v < min) {
            if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)
            || op.equals(Predicate.Op.NOT_EQUALS)) {
                return 1.0;
            }
            return 0.0;
        }
        if (v > max) {
            if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)
            || op.equals(Predicate.Op.NOT_EQUALS)) {
                return 1.0;
            }
            return 0.0;
        }
        int pos = (v - min) / bucketSize + 1;
        int numElements = 0;

        switch(op) {
            case EQUALS:
                numElements += bucketMap.get(pos) / bucketSize;
                break;
            case GREATER_THAN:
                for (int i = pos + 1; i <= buckets; i++) {
                    numElements += bucketMap.get(i);
                }
                numElements += (pos * bucketSize - (v - min + 1)) * bucketMap.get(pos) / bucketSize;
                break;
            case LESS_THAN:
                for (int i = 1; i <= pos - 1; i++) {
                    numElements += bucketMap.get(i);
                }
                numElements += ((v - min) - (pos - 1) * bucketSize) * bucketMap.get(pos) / bucketSize;
                break;
            case LESS_THAN_OR_EQ:
                for (int i = 1; i <= pos - 1; i++) {
                    numElements += bucketMap.get(i);
                }
                numElements += ((v - min) - (pos - 1) * bucketSize) * bucketMap.get(pos) / bucketSize;
                numElements += bucketMap.get(pos) / bucketSize;
                break;
            case GREATER_THAN_OR_EQ:
                for (int i = pos + 1; i <= buckets; i++) {
                    numElements += bucketMap.get(i);
                }
                numElements += (pos * bucketSize - (v - min)) * bucketMap.get(pos) / bucketSize;
                break;
            case NOT_EQUALS:
                numElements = totalElement - bucketMap.get(pos) / bucketSize;
                break;
        }
        if (totalElement == 0) {
            return 0.0;
        }
        return numElements * 1.0 / totalElement;
        /* my code for proj3 */
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        //return null;
        return bucketMap.toString();
    }
}
