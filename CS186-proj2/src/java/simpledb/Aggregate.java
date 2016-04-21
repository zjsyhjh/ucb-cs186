package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    /* my code for Aggregate */
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator res;
    private DbIterator resIt;
    /* my code for Aggregate */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	       // some code goes here
           /* my code for Aggregate */
           this.child = child;
           this.afield = afield;
           this.gfield = gfield;
           this.aop = aop;
           Type gtype = null;
           if (gfield != -1) {
               gtype = child.getTupleDesc().getFieldType(gfield);
           }
           if (child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)) {
               res = new IntegerAggregator(gfield, gtype, afield, aop);
           } else {
               res = new StringAggregator(gfield, gtype, afield, aop);
           }
           /* my code for Aggregate */
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	//return -1;
        /* my code for Aggregate */
        return gfield;
        /* my code for Aggregate */
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	//return null;
        /* my code for Aggregate */
        if (gfield == -1) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
        /* my code for Aggregate */
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	//return -1;
        /* my code for Aggregate */
        return afield;
        /* my code for Aggregate */
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	//return null;
        /* my code for Aggregate */
        return child.getTupleDesc().getFieldName(afield);
        /* my code for Aggregate */
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	//return null;
        /* my code for Aggregate */
        return aop;
        /* my code for Aggregate */
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        /* my code for Aggregate */
        super.open();
        child.open();
        while (child.hasNext()) {
            res.mergeTupleIntoGroup(child.next());
        }
        resIt = res.iterator();
        resIt.open();
        /* my code for Aggregate */
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	//return null;
        /* my code for Aggregate */
        if (resIt == null) {
            throw new DbException("must be opened first");
        }
        if (resIt.hasNext()) {
            return resIt.next();
        }
        return null;
        /* my code for Aggregate */
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        /* my code for Aggregate */
        child.rewind();
        /* my code for Aggregate */
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	//return null;
        return res.iterator().getTupleDesc();
    }

    public void close() {
	// some code goes here
        /* my code for Aggregate */
        super.close();
        child.close();
        /* my code for Aggregate */
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	//return null;
        /* my code for Aggregate */
        return new DbIterator[] {child};
        /* my code for Aggregate */
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        /* my code for Aggregate */
        children[0] = child;
        /* my code for Aggregate */
    }

}
