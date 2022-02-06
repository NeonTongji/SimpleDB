package simpledb.storage;


import simpledb.common.Database;
import simpledb.common.LockManager;
import simpledb.common.Permissions;
import simpledb.common.DbException;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private PageLruCache LRUPagesPool;


//    private final Page[] buffer;

    private HashMap<PageId, Page> pid2pages;

    private int PAGES_NUM;

    private LockManager lockManager;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final long SLEEP_INTERVAL;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
//        buffer = new Page[numPages];
        PAGES_NUM = numPages;
        pid2pages = new HashMap<>(numPages);
        LRUPagesPool = new PageLruCache(PAGES_NUM);
        lockManager = new LockManager();
        SLEEP_INTERVAL = 500; //睡眠时间太短会造成查询死锁频率过高
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.
     * If it is present, it should be returned.
     * If it is not present, it should be added to the buffer pool and returned.
     * If there is insufficient space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        // 遍历buffer，查看哪个是空的
//        if(pid2pages.containsKey(pid)) return pid2pages.get(pid);
//
//        if(pid2pages.size() == PAGES_NUM) {
//            evictPage();
//        }
//
//        // 从数据库\目录 创建数据表文件
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//        HeapPage page = (HeapPage) databaseFile.readPage(pid);
//        pid2pages.put(pid,page);
//        return page;

        boolean lock = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                : lockManager.grantXLock(tid, pid);

        // 自旋等待资源，一直到申请到锁
        while (!lock) {
            if(lockManager.deadLockOccurred(tid, pid)) {
                throw new TransactionAbortedException();
            }
            Thread.sleep(SLEEP_INTERVAL);

            lock = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                    : lockManager.grantXLock(tid, pid);
        }

        HeapPage heapPage = (HeapPage) LRUPagesPool.get(pid);
        if(heapPage != null) {
            return  heapPage; // 直接根据pid命中要查询的page
        }

        // 未命中，应该访问磁盘并将其缓存下来
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage newPage = (HeapPage) table.readPage(pid);
        Page removedPage = LRUPagesPool.put(pid, newPage);

        // 将要移除的最老的page，flush到磁盘
        if(removedPage != null) {
            try {
                flushPage(removedPage.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        // 成功了就释放了，没成功代表pid没被锁，或者tid没有锁这个pid
        if(!lockManager.unlock(tid, pid)) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     * 没有commit参数的默认为commit为真
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page
     * 判断tid是否持有对p的锁
     * */

    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.getLockState(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseTransactionLocks(tid);
        if(commit) {
            flushPages(tid);
        } else {
            revertTransactionAction(tid);
        }
    }

    /**
     * 事务回滚之前，撤销其对page的改变
     * @param tid
     */
    public void revertTransactionAction(TransactionId tid) {
        Iterator<Page> iterator = LRUPagesPool.iterator();
        while (iterator.hasNext()) {
            Page p = iterator.next();
            if(p.isDirty() != null && p.isDirty().equals(tid)) {
                // 还没有提交，从磁盘中获得源数据，替换到lruPool中
                LRUPagesPool.reCachePage(p.getId()); // 回滚事务，撤销其改变
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.insertTuple(tid, t);
        for(Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId tRecordId = t.getRecordId();
        HeapPageId tPageId = (HeapPageId) tRecordId.getPageId();
        int tableId = tPageId.getTableId();
        HeapFile hFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = hFile.deleteTuple(tid, t);
        for(Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> it = LRUPagesPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if(p.isDirty() != null) {
                flushPage(p.getId());
            }
        }
    }


    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        HeapPage dirtyPage = (HeapPage) pid2pages.get(pid);
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        table.writePage(dirtyPage);
        dirtyPage.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     * 将tid相关的dirty pages全部刷新到磁盘
     */

    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Page> iterator = LRUPagesPool.iterator();
        while (iterator.hasNext()) {
            Page page = iterator.next();
            if(page.isDirty() != null && page.isDirty().equals(tid)) { // isDirty()返回使该page dirty的tid, 如果clean, 返回null
                flushPage(page.getId());
                if(page.isDirty() == null) {
                    page.setBeforeImage();
                }
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 在LRU策略中已经实现
     */
    @Deprecated
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

}
