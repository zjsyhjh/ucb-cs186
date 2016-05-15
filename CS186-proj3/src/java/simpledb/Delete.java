package simpledb;

import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    /* my code for proj2 */
    private TransactionId t;
    private DbIterator child;
    private TupleDesc td;
    private boolean deleteOnce = false;
    /* my code for proj2 */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        /* my code for proj2 */
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Delete"});
        /* my code for proj2 */
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        /* my code for proj2*/
        return td;
        /* my code for proj2*/
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        /* my code for proj2 */
        if (deleteOnce) {
            return null;
        }
        int numOfTuples = 0;
        while (child.hasNext()) {
            try {
                numOfTuples++;
                Database.getBufferPool().deleteTuple(t, child.next());
            } catch(NoSuchElementException e) {
                e.printStackTrace();
            }
        }
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(numOfTuples));
        deleteOnce = true;
        return t;
        /* my code for proj2 */
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        //return null;
        /* my code for proj2 */
        return new DbIterator[]{child};
        /* my code for proj2 */
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        /* my code for proj2*/
        children[0] = child;
        /* my code for proj2*/
    }

}
