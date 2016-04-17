package simpledb;

import java.util.*;
import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */

    /* my code for SeqScan */
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator iterator;
    /* my code for SeqScan */

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        //return null;
        /* my code for SeqScan */
        return Database.getCatalog().getTableName(tableid);
        /* my code for SeqScan */
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        //return null;
        /* my code for SeqScan */
        return tableAlias;
        /* my code for SeqScan */
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        /* my code for SeqScan */
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        /* my code for SeqScan */
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        /* my code for SeqScan */
        iterator = Database.getCatalog().getDbFile(tableid).iterator(tid);
        iterator.open();
        /* my code for SeqScan */
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // return null;
        /* my code for SeqScan */
        TupleDesc oldtd = Database.getCatalog().getDbFile(tableid).getTupleDesc();
        Iterator<TDItem> it = oldtd.iterator();
        Type[] types = new Type[oldtd.numFields()];
        String[] names = new String[oldtd.numFields()];
        int index = 0;
        while (it.hasNext()) {
            TDItem item = it.next();
            types[index] = item.fieldType;
            names[index] = tableAlias + "." + item.fieldName;
            index++;
        }
        return new TupleDesc(types, names);
        /* my code for SeqScan */
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return false;
        /* my code for SeqScan */
        return iterator.hasNext();
        /* my code for SeqScan */
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        /* my code for SeqScan */
        return iterator.next();
        /* my code for SeqScan */
    }

    public void close() {
        // some code goes here
        /* my code for SeqScan */
        iterator.close();
        /* my code for SeqScan */
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        /* my code for SeqScan */
        iterator.rewind();
        /* my code for SeqScan */
    }
}
