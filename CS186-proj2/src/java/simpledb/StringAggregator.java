package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

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

    /* my code for StringAggregator */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> countMap;
    private TupleDesc td;
    /* my code for StringAggregator */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        /* my code for StringAggregator */
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.countMap = new HashMap<Field, Integer>();
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("what != COUNT");
        }
        /* my code for StringAggregator */
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        /* my code for StringAggregator */
        Field groupbyField;
        if (gbfield == Aggregator.NO_GROUPING) {
            groupbyField = new IntField(Aggregator.NO_GROUPING);
        } else {
            groupbyField = tup.getField(gbfield);
        }
        int value = 0;
        if (countMap.get(groupbyField) == null) {
            value = 1;
        } else {
            value = countMap.get(groupbyField) + 1;
        }
        countMap.put(groupbyField, value);
        if (td == null) {
            td = makeTupleDesc(tup);
        }
        /* my code for StringAggregator */
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for proj2");
        /* my code for StringAggregator */
        ArrayList<Tuple> arrayList = new ArrayList<Tuple>();
        for (Field f : countMap.keySet()) {
            Tuple tuple = new Tuple(td);
            if (gbfield == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(countMap.get(f)));
            } else {
                tuple.setField(0, f);
                tuple.setField(1, new IntField(countMap.get(f)));
            }
            arrayList.add(tuple);
        }
        return new TupleIterator(td, arrayList);
        /* my code for StringAggregator */
    }

    /* my code for StringAggregator */
    private TupleDesc makeTupleDesc(Tuple tuple) {
        Type[] typeAr;
        String[] fieldAr;
        if (gbfield == Aggregator.NO_GROUPING) {
            typeAr = new Type[1];
            fieldAr = new String[1];
            typeAr[0] = Type.INT_TYPE;
            fieldAr[0] = tuple.getTupleDesc().getFieldName(afield);
        } else {
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = tuple.getTupleDesc().getFieldType(gbfield);
            typeAr[1] = Type.INT_TYPE;
            fieldAr[0] = tuple.getTupleDesc().getFieldName(gbfield);
            fieldAr[1] = tuple.getTupleDesc().getFieldName(afield);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

}
