package simpledb;

import java.io.IOException;
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
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    class Pool{
        private Map<Integer, Page> m;

        Pool(int maxNumPages){
            m = new LinkedHashMap<>(maxNumPages, 0.75f, true);
        }

        public void put(int k, Page v){
            m.put(k, v);
        }

        public Page get(int k){
            return m.get(k);
        }

        public void remove(int k){
            m.remove(k);
        }

        public boolean containsKey(int k){
            return m.containsKey(k);
        }

        public Set<Integer> keySet(){
            return m.keySet();
        }
    }

//    class LRUPool <T> {
//        int remainingPairNum;
//        Map<Integer, T> m;
//
//        public LRUPool(int capacity) {
//            this.remainingPairNum = capacity;
//            this.m = new LinkedHashMap<>(remainingPairNum, 0.75f, true);
//        }
//
//        public T getPage(int pageHashCode) {
//            if(!m.containsKey(pageHashCode)) return null;
//            return m.get(pageHashCode);
//        }
//
//        public void putPage(int pageHashCode, T page) throws DbException {
//            logger.log("In putPage, remaining capacity is: " + this.remainingPairNum);
//            logger.log("Trying to cache page: " + pageHashCode);
//            if (this.remainingPairNum < 0)
//                throw new DbException("Buffer pool is full!");
//            boolean hasKey = this.m.containsKey(pageHashCode);
//            this.m.put(pageHashCode, page);
//            if(!hasKey){
//                logger.log("Page not in buffer pool");
//                this.remainingPairNum--;
//
//                while (this.remainingPairNum < 0) {
//                    logger.log("Remove page from buffer pool");
//                    evictPage(); // remove the page from the buffer pool
//                }
//            }
//        }
//
//        public List<T> getAllPages(){
//            List<T> allPages = new ArrayList<>();
//            for(int key : m.keySet())
//                allPages.add(m.get(key));
//            return allPages;
//        }
//    }

    private int maxNumPages;
    private int remainingPairNum;
//    private Map<Integer, Page> m;
    private Pool m;
    private List<Page> allPages;
    private LTM lockManager;

    private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log", false);
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxNumPages = numPages;
        remainingPairNum = maxNumPages;
//        m = new LinkedHashMap<>(remainingPairNum, 0.75f, true);
        m = new Pool(remainingPairNum);
        allPages = new LinkedList<>();
        lockManager = new LTM();
    }

    public Page getPage(int pageHashCode) {
        if(!m.containsKey(pageHashCode)) return null;
        return m.get(pageHashCode);
    }

    public void putPage(int pageHashCode, Page page) throws DbException {
        logger.log("In putPage, remaining capacity is: " + this.remainingPairNum);
        logger.log("Trying to cache page: " + pageHashCode);
        if (this.remainingPairNum < 0)
            throw new DbException("Buffer pool is full!");
        boolean hasKey = m.containsKey(pageHashCode);
        m.put(pageHashCode, page);
        allPages.add(page);
        if(!hasKey){
            logger.log("Page not in buffer pool");
            this.remainingPairNum--;

            while (this.remainingPairNum < 0) {
                logger.log("Remove page from buffer pool");
                evictPage(); // remove the page from the buffer pool
            }
        }
    }

    public synchronized List<Page> getAllPages(){
//        List<Page> allPages = new ArrayList<>();
//        for(int key : m.keySet())
//            allPages.add(m.get(key));
        return allPages;
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
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Catalog catalog = Database.getCatalog();
        HeapFile dbFile;
        try {
            dbFile = (HeapFile) catalog.getDatabaseFile(pid.getTableId());

        } catch (NoSuchElementException e) {
            e.printStackTrace();
            throw new DbException("Can not get DbFile for table with ID: " + pid.getTableId());
        }
        logger.log("----Try to read page in buffer pool: " + pid.hashCode() + "----");
        HeapPage pg = (HeapPage) getPage(pid.hashCode()); // check if page is already in the pool
        if(pg != null) {
            logger.log("Page in buffer pool!");
            return pg;
        }
        logger.log("Not in buffer pool");
        // read page from the disk
        pg = (HeapPage) dbFile.readPage(pid);
        // store the newly read page into the buffer pool
        putPage(pid.hashCode(), pg);
        logger.log("----End of read page in buffer pool----");
        return pg;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
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
        HeapFile databaseFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = databaseFile.insertTuple(tid, t);
        logger.log("modifiedPages size: " + modifiedPages.size());
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            putPage(page.getId().hashCode(), (HeapPage) page);
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        Page p =getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        ((HeapPage) p).deleteTuple(t);
        p.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (Page page : getAllPages())
            flushPage(page.getId());
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
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page pageToFlush = getPage(pid.hashCode());
        TransactionId dirtyTransactionId = pageToFlush.isDirty();
        if (dirtyTransactionId != null)
        {
            HeapFile databaseFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            databaseFile.writePage(pageToFlush);
            pageToFlush.markDirty(false, dirtyTransactionId);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int k = m.keySet().iterator().next();
        Page pageToEvict = m.get(k);
        try{
            flushPage(pageToEvict.getId());
        }catch (IOException e){
            e.printStackTrace();
            throw new DbException("IO exception happens when trying to flush and evict a page to the disk");
        }

        m.remove(k);
        allPages.remove(pageToEvict);
        remainingPairNum++;
    }
}
