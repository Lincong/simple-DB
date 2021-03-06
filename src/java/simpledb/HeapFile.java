package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

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
    private File f;
    private TupleDesc td;
    private int ID;
    private BufferPool bufferPool;
    private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log", false);
    private Semaphore synchronizer = new Semaphore(1, true);

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        ID = f.getAbsoluteFile().hashCode();
        bufferPool = Database.getBufferPool();
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
        return ID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pid.getPageNumber();

        byte[] pageData = new byte[pageSize];
        try {
            RandomAccessFile f = new RandomAccessFile(this.f,"r");
            if (offset + pageSize > f.length()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
            }
            f.seek(offset);
            f.readFully(pageData);
            f.close();
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            throw new IllegalArgumentException();
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            throw new IllegalArgumentException();
        }

        HeapPage readPage;
        try {
            readPage = new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Creating new page failed for file: " + f.toString());
            return null;
        }
        return readPage;
    }

    private void printByteArray(byte[] data, int len) {
        if(len <= 0)
            len = data.length;
        System.out.println("Byte array: ");
        for (byte b : data) {
            if(len <= 0)
                break;
            len--;
            System.out.println(Integer.toBinaryString(b & 255 | 256).substring(1));
        }
        System.out.println("Byte array end ---");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        int offset = BufferPool.getPageSize() * page.getId().getPageNumber();
        raf.seek(offset);
        byte[] newHeapPageData = page.getPageData();
        // the modified new page is already written to disk. So no need to mark it as dirty
        raf.write(newHeapPageData, 0, BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long totalBytes = f.length();
        if(totalBytes % BufferPool.getPageSize() != 0)
            System.out.println("That's weird. Why is the file size not the multiple of page size");

        return (int) (totalBytes / BufferPool.getPageSize());
    }

    private HeapPage getFreePage(TransactionId tid) throws TransactionAbortedException, DbException {
        logger.log("Trying to find a free page for transaction " + tid);
        int totalPageNum = this.numPages();
        logger.log("totalPageNum: " + totalPageNum);
        if(totalPageNum == 0){
            logger.log("no page yet");
            return null;
        }
        for (int i = 0; i < totalPageNum; i++) {
            logger.log("i: " + i);
            PageId pid = new HeapPageId(this.getId(), i);
            logger.log("current pageID " + pid);
            HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (hpage.getNumEmptySlots() > 0)
                return hpage;
            else // otherwise release the lock on the page since the page is not going to be used
                Database.getBufferPool().releasePage(tid, hpage.getId());
        }
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        logger.log("In HeapFile insertTuple() ");
        logger.log("tid: " + tid);
        logger.log("tuple to insert " + t);
        HeapPage p = getFreePage(tid);
        if (p != null) {
            logger.log("found available page to insert");
            logger.log("insert to page: " + p.getId());
            // check the
            RecordId rid = new RecordId(p.getId(), -1);
            t.setRecordId(rid);
            p.insertTuple(t);
            p.markDirty(true, tid);
            logger.log("end of insertTUple()");
            return new ArrayList<> (Arrays.asList(p));
        }
        // no empty pages found, so create a new one
        int newPageNum = this.numPages();
        synchronizerOn();
        HeapPageId newHeapPageId = new HeapPageId(this.getId(), newPageNum);
        BufferPool bpool = Database.getBufferPool();
        try {
            p = new HeapPage(newHeapPageId, HeapPage.createEmptyPageData());
            p.insertTuple(t);
            logger.log("newHeapPage ID: " + p.getId());
            writePage(p);
            bpool.lockPage(tid, newHeapPageId, Permissions.READ_WRITE);
            bpool.putPage(newHeapPageId.hashCode(), p);
        } finally {
            synchronizerOff();
        }
        logger.log("end of insertTUple()");
        return new ArrayList<> (Arrays.asList());
    }

    private void synchronizerOn(){
        try {
            synchronizer.acquire();
        } catch (InterruptedException e){
            e.printStackTrace();
            System.out.println("This should not happen");
            System.exit(1);
        }
    }

    private void synchronizerOff(){
        synchronizer.release();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        Page p = bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
        ((HeapPage) p).deleteTuple(t);
        p.markDirty(true, tid);
        return new ArrayList<Page> (Arrays.asList(p));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    private HeapFile heapFile;
    private TransactionId tid;
    private Iterator<Tuple> pageIterator;
    private BufferPool bufferPool;
    private int totalPageNum;
    private HeapPage currPage;
    private int currPageNum;

    HeapFileIterator(HeapFile heapFile, TransactionId tid){
        this.heapFile = heapFile;
        this.tid = tid;
        totalPageNum = this.heapFile.numPages();
        pageIterator = null;
        bufferPool = Database.getBufferPool();
        currPage = null;
        currPageNum = 0;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page
     */
    public void open() throws DbException, TransactionAbortedException {
        // get the first pagae in this file (table)
        if(!getIteratorForNextPage())
            throw new DbException("How come can't even get the tuple iterator on the first page when open is called?");
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if(pageIterator == null) return null;
        Tuple t;
        if(pageIterator.hasNext()) {
            t = pageIterator.next();

        } else {
            // release the lock on the current page before get a new page from the buffer pool
            if(currPage.isDirty() == null)
                Database.getBufferPool().releasePage(tid, currPage.getId());
            if(!getIteratorForNextPage()) // no next tuple to read (maybe)
                return null;
            t = (pageIterator.hasNext() ? pageIterator.next() : null);
        }
        return t;
    }

    private boolean getIteratorForNextPage() throws DbException, TransactionAbortedException {
        if(currPageNum >= totalPageNum)
            return false;
        HeapPageId pageToReadID = new HeapPageId(heapFile.getId(), currPageNum);
        currPage = (HeapPage) bufferPool.getPage(tid, pageToReadID, Permissions.READ_ONLY);
        pageIterator = currPage.iterator();
        currPageNum++;
        return true;
    }

    public void close() {
        // Ensures that a future call to next() will fail
        // next = null;
        if(currPage.isDirty() == null)
            Database.getBufferPool().releasePage(tid, currPage.getId());
        currPageNum = 0; // reset
        pageIterator = null;
        super.close();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}

