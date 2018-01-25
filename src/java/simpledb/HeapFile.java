package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

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

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        ID = f.getAbsoluteFile().hashCode();
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
        Reader fr;
        int pageSize = BufferPool.getPageSize();
        try {
            fr = new FileReader(f);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("HeapFile not found: " + f.toString());
            return null;
        }

        try {
            int offset = pageSize * pid.getPageNumber();
            if(offset != fr.skip(offset)) {
                System.out.println("How come we can't skip " + offset + "bytes for file: " + f.toString());
                return null;
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Offset failed for file: " + f.toString());
            return null;
        }
        byte[] data = new byte[pageSize];
        try {
            for (int i = 0; i < pageSize; i++) {
                data[i] = (byte) fr.read();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Reading byte failed for file: " + f.toString());
        }

        HeapPage readPage;
        try {
            readPage = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Creating new page failed for file: " + f.toString());
            return null;
        }
        return readPage;
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
        // some code goes here
        long totalBytes = f.length();
        if(totalBytes % BufferPool.getPageSize() != 0)
            System.out.println("That's weird. Why is the file size not the multiple of page size");

        return (int) (totalBytes / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
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
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    private HeapFile heapFile;
    private TransactionId tid;
    private Iterator<Tuple> pageIterator;
    private BufferPool bufferPool;
    private int totalPageNum;
    private int currPageNum;

    HeapFileIterator(HeapFile heapFile, TransactionId tid){
        this.heapFile = heapFile;
        this.tid = tid;
        totalPageNum = this.heapFile.numPages();
        pageIterator = null;
        bufferPool = Database.getBufferPool();
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
        Tuple t;
        if(pageIterator.hasNext()) {
            t = pageIterator.next();

        } else {
            if(!getIteratorForNextPage()) // no next tuple to read (maybe)
                return null;
            t = pageIterator.next();
        }
        return t;
    }

    private boolean getIteratorForNextPage() throws DbException, TransactionAbortedException {
        currPageNum++;
        if (currPageNum >= totalPageNum)
            return false;
        HeapPageId pageToReadID = new HeapPageId(heapFile.getId(), currPageNum);
        HeapPage newPage = (HeapPage) bufferPool.getPage(tid, pageToReadID, Permissions.READ_ONLY);
        pageIterator = newPage.iterator();
        return true;
    }

    public void close() {
        // Ensures that a future call to next() will fail
        // next = null;
        currPageNum = 0; // reset
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

