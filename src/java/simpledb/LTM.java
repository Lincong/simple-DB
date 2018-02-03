package simpledb;
/**
 * Created by lincongli on 2/2/18.
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// a k-v map: PID -> page state
// page state (only one thread is allowed to r/w the page state at a time):
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

            public boolean isTransaction(TransactionId tid){
                return this.tid.equals(tid);
            }
            public void interruptTransactionThread(){
                thread.interrupt();
            }

        }
//        List<TransactionThread> readingTransactions;

        Map<TransactionId, TransactionThread> readingTransactions;
        Map<TransactionId, TransactionThread> writingTransactions;
        Semaphore stateLock;
        ReadWriteLock rwLock;
        private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log", false);

        PageState(){
            readingTransactions = new HashMap<>();
            writingTransactions = new HashMap<>();
            stateLock = new Semaphore(1, true);
            rwLock = new ReentrantReadWriteLock();
        }

        public void lock(TransactionId tid, Permissions perm)
                throws TransactionAbortedException, DbException {
            boolean requestingReadLock = (perm == Permissions.READ_ONLY);
            acquireStateLock();
            boolean hasReadLock = readingTransactions.containsKey(tid);
            boolean hasWriteLock = writingTransactions.containsKey(tid);
            // make sure one transaction only has at most one lock
            assert !(hasReadLock && hasWriteLock);
            if(hasWriteLock){
                releaseStateLock();
                throw new DbException("transaction " + tid + " is requesting write lock but it already has the lock");
            }

            // TODO: check if there is any transaction that needs to be aborted
            // check request redundant lock
            if(hasReadLock && requestingReadLock){
                releaseStateLock();
                throw new DbException("transaction " + tid + " is requesting read lock but it already has the lock");
            }
            // only 2 cases are allowed
            // 1. read lock -> write lock
            // 2. no lock   -> r/w lock
            releaseStateLock();
            try {
                if (perm == Permissions.READ_ONLY) { // read lock
                    rwLock.readLock().lockInterruptibly();

                } else { // write lock
                    if(hasReadLock){ // if transaction already has the reading lock
                        unlock(tid, Permissions.READ_ONLY);
                    }
                    rwLock.writeLock().lockInterruptibly();

                }
                // add the current lock request to the corresponding hash map
                acquireStateLock();
                TransactionThread tt = new TransactionThread(tid, Thread.currentThread());
                (perm == Permissions.READ_ONLY ? readingTransactions : writingTransactions).put(tid, tt);
                releaseStateLock();

            }catch (InterruptedException e){
                // if a thread is interrupted that means the transaction in this thread should abort
                throw new TransactionAbortedException();
            }
        }

        public void unlock(TransactionId tid, Permissions perm)
                throws DbException {

            acquireStateLock();
            if(perm == Permissions.READ_ONLY){ // unlock read lock
                if(!readingTransactions.containsKey(tid)){
                    releaseStateLock();
                    throw new DbException("Transaction " + tid + " does not hold any read lock!");
                }
                rwLock.readLock().unlock();
                readingTransactions.remove(tid);

            }else{ // unlock write lock
                if(!writingTransactions.containsKey(tid)){
                    releaseStateLock();
                    throw new DbException("Transaction " + tid + " does not hold any read lock!");
                }
                rwLock.writeLock().unlock();
                writingTransactions.remove(tid);
            }
            readingTransactions.remove(tid);
            releaseStateLock();
        }

        public boolean holdsLock(TransactionId tid){
            acquireStateLock();
            boolean holdingLock = (readingTransactions.containsKey(tid)
                                || writingTransactions.containsKey(tid));
            releaseStateLock();
            return holdingLock;
        }

        private void acquireStateLock(){
            try {
                stateLock.acquire();
            } catch (InterruptedException e){
                e.printStackTrace();
                System.out.println("This should not happen");
                System.exit(1);
            }
        }

        private void releaseStateLock(){
            stateLock.release();
        }
    }

    ConcurrentMap<PageId, PageState> lockTable;

    public LTM(){
        this.lockTable = new ConcurrentHashMap<>();
    }

    public boolean getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        if(!lockTable.containsKey(pid))
        return true;
    }

    public void returnLock(TransactionId tid, PageId pid)
            throws DbException {

    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        if(!lockTable.containsKey(pid))
            return false;
        return lockTable.get(pid).holdsLock(tid);
    }
}
