package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int numPage;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.numPage = (int) (f.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            int pos = BufferPool.getPageSize() * pid.getPageNumber();
            RandomAccessFile r = new RandomAccessFile(f, "r");
            /**
             * Sets the file-pointer offset measured from the beginning of this file,
             * at which the next read or write occurs.The offset may be set beyond the end of the file.
             * Setting the offset beyond the end of the file does not change the file length.
             * The file length will change only by writing after the offset has been set beyond the end of the file.
             */
            r.seek(pos);
            /**
             * Reads a byte of data from this file. The byte is returned as an integer in the range 0 to 255 (0x00-0x0ff). This method blocks if no input is yet available.
             * Although RandomAccessFile is not a subclass of InputStream, this method behaves in exactly the same way as the InputStream.read() method of InputStream.
             * Returns:
             * the next byte of data, or -1 if the end of the file has been reached.
             * Throws:
             * IOException ??? if an I/O error occurs. Not thrown if end-of-file has been reached.
             */
            r.read(data, 0, data.length);
            HeapPage heapPage = new HeapPage((HeapPageId) pid, data);
            return heapPage;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // done
        return numPage;
    }

    // see DbFile.java for javadocs

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // ??????pages??? ??????header???????????????
        List<Page> list = new ArrayList<>();
        for(int i = 0; i < numPage; i++) {
            HeapPageId pid = new HeapPageId(getId(), i); //??????????????????pid
            HeapPage heapPage = null;
            try {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(heapPage.getNumEmptySlots() != 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                list.add(heapPage);
                break;
            }
        }
        // ??????????????????????????????????????????????????????????????????????????????????????????????????????slot?????????????????????????????????????????????
        // ?????????????????????????????????new HeapPage;
        // ???numPage??????
        // ???page???????????????????????????????????????????????????

        if(list.size() == 0) { // ?????????????????????????????????pages?????????
            // ??????????????????page??????numPages
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            HeapPage blankPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            numPage++; // page??????????????????
            writePage(blankPage);
            HeapPage page = null;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            page.insertTuple(t);
            page.markDirty(true, tid);
            list.add(page);
        }

        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage hpage = null;
        for(int i = 0; i < numPages(); i++) {
            if(i == pid.getPageNumber()) {
                try {
                    hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                hpage.deleteTuple(t);
                 list.add(hpage);
            }
        }
        if(hpage == null) {
            throw new DbException("???tuple???????????????table");
        }
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{

        private TransactionId tid;
        private Iterator<Tuple> tupleIterator;
        private int pagePos;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            tupleIterator = getTuplesInPage(pid);
        }

        private Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            // ????????????????????????????????????????????????????????????page
            // ????????????HeapFile???readPage??????????????????BufferPool?????????page??????????????????????????????
            HeapPage page = null;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // ????????????????????????
            if(tupleIterator == null) return false;
            // ???????????????tuple???????????????
            if(tupleIterator.hasNext()) return true;
            // ????????????????????????????????????
            if(pagePos < numPage - 1) {
                pagePos++; // ???????????????
                HeapPageId nextPid = new HeapPageId(getId(), pagePos);
                tupleIterator = getTuplesInPage(nextPid);
                return tupleIterator.hasNext();
            }
            // ???????????????
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //????????????????????????tupleIterator.hasNext()??? tupleIterator.hasNext() ????????????????????????????????????
            if(!hasNext()) {
                throw new NoSuchElementException("??????????????????????????????????????????");
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
//            HeapPageId pageId = new HeapPageId(getId(), 0);
//            tupleIterator = getTuplesInPage(pageId);
            open();
        }

        @Override
        public void close() {
            // ????????????????????????0????????????????????????null
            pagePos = 0;
            tupleIterator = null;
        }
    }

}

