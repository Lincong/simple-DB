package simpledb;
/**
 * Created by lincongli on 2/2/18.
 */

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

// a k-v map: PID -> page state
// page state:
    // 1. a list of transactions holding read lock on the page
    // 2. a list of transactions holding write lock on the page
    //
public class LTM {

    class PageState {

        class TransactionThread{
            TransactionId tid;
            Thread thread;

            public TransactionThread(TransactionId tid, Thread thread){
                this.tid = tid;
                this.thread = thread;
            }

            public void interruptTransactionThread(){
                thread.interrupt();
            }
        }
        List<TransactionThread> readingTransactions;
        List<TransactionThread> writingTransactions;
        Semaphore stateLock;
        Semaphore rwLock;

        PageState(){
            readingTransactions = new LinkedList<>();
            writingTransactions = new LinkedList<>();
            stateLock = new Semaphore(1, true);
            rwLock = new Semaphore(Integer.MAX_VALUE, true);
        }

        public boolean lock(TransactionId tid, Permissions perm)
                throws TransactionAbortedException, DbException {

            return true;
        }

        public void unlock(Transaction tid, Permissions perm)
                throws DbException {

        }
    }

    ConcurrentMap<PageId, PageState> concurrentMap;

    public LTM(){
        this.concurrentMap = new ConcurrentHashMap<>();
    }

    public boolean getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        return true;
    }

    public void returnLock(Transaction tid, PageId pid, Permissions perm)
            throws DbException {

    }
}
