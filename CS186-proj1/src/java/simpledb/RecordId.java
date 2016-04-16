package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */

    /* my code for RecordId */
    private PageId pid;
    private int tupleno;
    /* my code for RecordId */

    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        /* my code for RecordId */
        this.pid = pid;
        this.tupleno = tupleno;
        /* my code for RecordId */
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        //return 0;
        /* my code for RecordId */
        return this.tupleno;
        /* my code for RecordId */
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        //return null;
        /* my code for RecordId */
        return this.pid;
        /* my code for RecordId */
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        /* my code for RecordId */
        if (o instanceof RecordId) {
            RecordId rid = (RecordId)o;
            if (this.pid.equals(rid.pid) && this.tupleno == rid.tupleno) {
                return true;
            }
            return false;
        }
        return false;
        /* my code for RecordId */
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        /* my code for RecordId */
        return pid.hashCode() * tupleno;
        /* my code for RecordId */
    }

}
