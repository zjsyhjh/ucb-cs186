package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */

    /* my code for HeapPageId */
    private int tableId;
    private int pgNo;
    /* my code for HeapPageId */

    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        /* my code for HeapPageId */
        this.tableId = tableId;
        this.pgNo = pgNo;
        /* my code for HeapPageId */
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        //return 0;
        /* my code for HeapPageId */
        return this.tableId;
        /* my code for HeapPageId */
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        //return 0;
        /*my code for HeapPageId */
        return this.pgNo;
        /*my code for HeapPageId */
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        /* my code for HeapPageId */
        return tableId * pgNo;
        /* my code for HeapPageId */
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        //return false;
        /* my code for HeapPageId */
        if (o instanceof PageId) {
            HeapPageId pid = (HeapPageId)o;
            if (this.tableId == pid.tableId && this.pgNo == pid.pgNo) {
                return true;
            }
            return false;
        }
        return false;
        /* my code for HeapPageId */
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
