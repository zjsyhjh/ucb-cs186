package simpledb;

import java.util.*;
import org.omg.CORBA.INTF_REPOS;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    /* my code for IntegerAggregator */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> map;
    private HashMap<Field, Integer> countMap;
    private HashMap<Field, Integer> sumMap;
    private TupleDesc td;
    /* my code for IntegerAggregator */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        /* my code for IntegerAggregator */
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        map = new HashMap<Field, Integer>();
        countMap = new HashMap<Field, Integer>();
        sumMap = new HashMap<Field, Integer>();
        /* my code for IntegerAggregator */
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        /* my code for IntegerAggregator */
        Field groupbyField = null;
        if (gbfield == Aggregator.NO_GROUPING) {
            groupbyField = new IntField(Aggregator.NO_GROUPING);
        } else {
            groupbyField = tup.getField(gbfield);
        }

        int value = 0;
        switch(what) {
            case MIN:
                if (map.get(groupbyField) == null) {
                    value = (IntField)tup.getField(afield).getValue();
                } else {
                    value = Math.min((IntField)tup.getField(afield).getValue(), map.get(groupField));
                }
                break;
            case MAX:
                if (map.get(groupbyField) == null) {
                    value = (IntField)tup.getField(afield).getValue();
                } else {
                    value = Math.max((IntField)tup.getField(afield).getValue(), map.get(groupbyField));
                }
                break;
            case SUM:
                if (map.get(groupbyField) == null) {
                    value = (IntField)tup.getField(afield).getValue();
                } else {
                    value = map.get(groupbyField) + (IntField)tup.getField(afield).getValue();
                }
                break;
            case AVG:
                if (countMap.get(groupbyField) == null) {
                    countMap.put(groupbyField, 1);
                    sumMap.put(groupbyField, (IntField)tup.getField(afield).getValue());
                } else {
                    value = sumMap.get(groupbyField) + (IntField)tup.getField(afield).getValue();
                    sumMap.put(groupbyField, value);
                    int count = countMap.get(groupbyField) + 1;
                    countMap.put(groupbyField, count);
                }
                value = value / count;
                break;
            case COUNT:
                if (map.get(groupbyField) == null) {
                    value = 1;
                } else {
                    value = map.get(groupbyField) + 1;
                }
                break;
        }

        map.put(groupbyField, value);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for proj2");
    }

}
