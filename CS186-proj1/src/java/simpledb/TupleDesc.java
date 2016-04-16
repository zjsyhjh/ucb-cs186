package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.Iterator;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /* my code for TupleDesc */
    private TDItem[] items = null;
    /* my code for TupleDesc */

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return null;
        /* my code for TupleDesc */
        Iterator<TDItem> it = new Iterator<TDItem>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < numFields();
            }
            @Override
            public TDItem next() {
                TDItem i = items[index];
                index++;
                return i;
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException("implement this");
            }
        };
        return it;
        /* my code for TupleDesc */
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        /* my code for TupleDesc */
        items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
        /* my code for TupleDesc */
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        /* my code for TupleDesc */
        items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i], "");
        }
        /* my code for TupleDesc */
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        //return 0;
        /* my code for TupleDesc */
        return items.length;
        /* my code for TupleDesc */
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        /* my code for TupleDesc */
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException();
        }
        return items[i].fieldName;
        /* my code for TupleDesc */
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        /* my code for TupleDesc */
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException();
        }
        return items[i].fieldType;
        /* my code for TupleDesc */
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        //return 0;
        /* my code for TupleDesc */
        int index = -1;
        for (int i = 0; i < numFields(); i++) {
            if (items[i].fieldName == name) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            throw new NoSuchElementException();
        } else {
            return index;
        }
        /* my code for TupleDesc */
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //return 0;
        /*my code for TupleDesc */
        int bytesOfTupleDesc = 0;
        for (TDItem i : items) {
            bytesOfTupleDesc += (i.fieldType).getLen();
        }
        return bytesOfTupleDesc;
        /*my code for TupleDesc */
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //return null;
        /*my code for TupleDesc */
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        int index = 0;
        for (TDItem i : td1.items) {
            typeAr[index] = i.fieldType;
            fieldAr[index] = i.fieldName;
            index++;
        }
        for (TDItem i : td2.items) {
            typeAr[index] = i.fieldType;
            fieldAr[index] = i.fieldName;
            index++;
        }
        return new TupleDesc(typeAr, fieldAr);
        /*my code for TupleDesc */
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        //return false;
        /* my code for TupleDesc */
        if (o instanceof TupleDesc) {
            TupleDesc td = (TupleDesc)o;
            if (this.numFields() == td.numFields()) {
                for (int i = 0; i < numFields(); i++) {
                    if (this.items[i].fieldType != td.items[i].fieldType) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        return false;
        /* my code for TupleDesc */
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        //return "";
        /* my code for TupleDesc */
        String str = "";
        for (TDItem i : items) {
            str += (i.fieldType).toString() + "(" + i.fieldName +")" + ", ";
        }
        return str;
        /* my code for TupleDesc */
    }
}
