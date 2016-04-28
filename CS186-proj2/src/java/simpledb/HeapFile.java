package simpledb;

import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.acl.Permission;
import java.util.*;
import java.util.ArrayList;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    /* my code for HeapFile */
    private File f;
    private TupleDesc td;
    /* my code for HeapFile */

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        //return null;
        /* my code for HeapFile */
        return f;
        /* my code for HeapFile */
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        /* my code for HeapFile */
        return f.getAbsoluteFile().hashCode();
        /* my code for HeapFile */
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        /* my code for HeapFile */
        return td;
        /* my code for HeapFile */
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //return null;
        /* my code for HeapFile */
        RandomAccessFile rf = null;
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        HeapPage page = null;
        try {
            rf = new RandomAccessFile(f, "r");
            rf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
            rf.read(data);
            rf.close();
            page = new HeapPage((HeapPageId)pid, data);
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return page;
        /* my code for HeapFile */
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //return 0;
        /* my code for HeapFile */
        return (int)Math.ceil(f.length() * 1.0 / BufferPool.PAGE_SIZE);
        /* my code for HeapFile */
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for proj1
        /* my code for proj2 */
        ArrayList<Page> pageList = new ArrayList<Page>();
        BufferPool bufferPool = Database.getBufferPool();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage)bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                pageList.add(page);
                break;
            }
        }
        //
        if (pageList.isEmpty()) {
            PageId pid = new HeapPageId(getId(), numPages());
            try {
                byte[] bytes = HeapPage.createEmptyPageData();
                RandomAccessFile file = new RandomAccessFile(getFile(), "rw");
                file.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
                file.write(bytes);
            } catch(FileNotFoundException e) {
                e.printStackTrace();
            }
            HeapPage page = (HeapPage)bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            pageList.add(page);
        }
        return pageList;
        /* my code for proj2 */
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for proj1
        /* my code for proj2 */
        /* don't need to delete tuple in file ? */
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage page = (HeapPage)bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return page;
        /* my code for proj2 */
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //return null;
        /* my code for HeapFile */
        return new HeapFileIterator(tid);
        /* my code for HeapFile */
    }

    /* my code for HeapFile */
    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private Iterator<Tuple> iterator = null;
        private int pageIndex;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.pageIndex = 0;
            this.isOpen = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            HeapPageId pid = new HeapPageId(getId(), pageIndex);
            HeapPage page = (HeapPage)((Database.getBufferPool()).getPage(tid, pid, Permissions.READ_ONLY));
            iterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (isOpen) {
                if (iterator == null) {
                    return false;
                }
                if (iterator.hasNext()) {
                    return true;
                }

                while (pageIndex < numPages() - 1) {
                    pageIndex++;
                    HeapPageId pid = new HeapPageId(getId(), pageIndex);
                    HeapPage page = (HeapPage)((Database.getBufferPool()).getPage(tid, pid, Permissions.READ_ONLY));
                    iterator = page.iterator();
                    if (iterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (isOpen) {
                if (hasNext()) {
                    return iterator.next();
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pageIndex = 0;
            isOpen = false;
            iterator = null;
        }
    }
    /* my code for HeapFile */

}
