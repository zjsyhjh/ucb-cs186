package simpledb;

import java.util.*;
import javax.sql.rowset.Predicate;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    /* my code for Filter */
    private Predicate p;
    private DbIterator child;
    /* my code for Filter */

    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        /* my code for Filter */
        this.p = p;
        this.child = child;
        /* my code for Filter */
    }

    public Predicate getPredicate() {
        // some code goes here
        //return null;
        /* my code for Filter */
        return p;
        /* my code for Filter */
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        /* my code for Filter */
        return child.getTupleDesc();
        /* my code for Filter */
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        /* my code for Filter */
        child.open();
        super.open();
        /* my code for Filter */
    }

    public void close() {
        // some code goes here
        /* my code for Filter */
        child.close();
        super.close();
        /* my code for Filter */
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        /* my code for Filter */
        child.rewind();
        /* my code for Filter */
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
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
        //return null;
        /* my code for Filter */
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (p.filter(tuple)) {
                return tuple;
            }
        }
        return null;
        /* my code for Filter */
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        //return null;
        /* my code for Filter */
        return new DbIterator[] {child};
        /* my code for Filter */
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        /* my code for Filter */
        if (children != null) {
            children[0] = child;
        }
        /* my code for Filter */
    }

}
