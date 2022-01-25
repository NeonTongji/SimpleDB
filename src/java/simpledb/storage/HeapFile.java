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
             * IOException – if an I/O error occurs. Not thrown if end-of-file has been reached.
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // done
        return numPage;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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
            // 要获得元组的迭代器，首先要获得元组所在的page
            // 不能使用HeapFile的readPage方法，要使用BufferPool来获得page，才能实现缓存功能；
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // 检查是否开启迭代
            if(tupleIterator == null) return false;
            // 当前页还有tuple没有被遍历
            if(tupleIterator.hasNext()) return true;
            // 检查是否还有下一页要遍历
            if(pagePos < numPage - 1) {
                pagePos++; // 跑到下一页
                HeapPageId nextPid = new HeapPageId(getId(), pagePos);
                tupleIterator = getTuplesInPage(nextPid);
                return tupleIterator.hasNext();
            }
            // 没有下一页
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //注意这里不能是（tupleIterator.hasNext()） tupleIterator.hasNext() 是判断当前页是否迭代完毕
            if(!hasNext()) {
                throw new NoSuchElementException("无法开启迭代，或已经迭代完毕");
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
            pagePos = 0;
            tupleIterator = null;
        }
    }

}

