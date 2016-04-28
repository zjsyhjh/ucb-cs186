package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    /* my code proj2 */
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean insertOnce = false;
    private TupleDesc td;
    /* my code proj2 */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
            throw new DbException("The TupleDesc of child differs from table into which we are to insert");
        }
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Insert"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        /* my code for proj2 */
        return td;
        /* my code for proj2 */
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        /* my code for proj2 */
        super.open();
        child.open();
        /* my code for proj2 */
    }

    public void close() {
        // some code goes here
        /* my code for proj2 */
        super.close();
        child.close();
        /* my code for proj2 */
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        /* my code for proj2 */
        child.rewind();
        /* my code for proj2 */
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        /* my code for proj2 */
        if (insertOnce) {
            return null;
        }
        int numOfTuples = 0;
        while (child.hasNext()) {
            try {
                numOfTuples++;
                Database.getBufferPool().insertTuple(t, tableid, child.next());
            } catch(NoSuchElementException e) {
                e.printStackTrace();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(numOfTuples));
        insertOnce = true;
        return t;
        /* my code for proj2 */
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        //return null;
        /* my code for proj2 */
        return new DbIterator[] {child};
        /* my code for proj2 */
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        /* my code for proj2 */
        children[0] = child;
        /* my code for proj2 */
    }
}
