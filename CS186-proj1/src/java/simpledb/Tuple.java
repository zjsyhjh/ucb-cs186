package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /* my code for Tuple */
    private TupleDesc td;
    private RecordId rid;
    private Field[] fields = null;
    /* my code for Tuple */

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        /* my code for Tuple */
        this.td = td;
        this.fields = new Field[td.numFields()];
        /* my code for Tuple */
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        /* my code for Tuple */
        return this.td;
        /* my code for Tuple */
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        //return null;
        /*my code for Tuple */
        return rid;
        /*my code for Tuple */
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        /* my code for Tuple */
        this.rid = rid;
        /* my code for Tuple */
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        /* my code for Tuple */
        this.fields[i] = f;
        /* my code for Tuple */
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        //return null;
        /* my code for Tuple */
        return this.fields[i];
        /* my code for Tuple */
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        /* my code for Tuple */
        String str = "";
        for (int i = 0; i < fields.length - 1; i++) {
            str += fields[i].toString() + " ";
        }
        str += fields[fields.length - 1] + "\n";
        return str;
        /* my code for Tuple */
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        //return null;
        /* my code for Tuple */
        Iterator<Field> it = new Iterator<Field>() {
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < fields.length;
            }
            @Override
            public Field next() {
                Field f = fields[index];
                index++;
                return f;
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException("implement this");
            }
        };
        return it;
        /* my code for Tuple */
    }
}
