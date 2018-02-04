package simpledb;

/**
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

        private Map<TransactionId, TransactionThread> readingTransactions;
        private Map<TransactionId, TransactionThread> writingTransactions;
        private Semaphore stateLock;
        private ReadWriteLock rwLock;
        private PageId pid;
        private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log", true);

        PageState(PageId pid){
            readingTransactions = new HashMap<>();
            writingTransactions = new HashMap<>();
            stateLock = new Semaphore(1, true);
            rwLock = new ReentrantReadWriteLock();
            this.pid = pid;
        }

        public void lock(TransactionId tid, Permissions perm)
                throws TransactionAbortedException, DbException {
            boolean requestingReadLock = (perm == Permissions.READ_ONLY);
            logger.log("In lock(), transaction " + tid + " trying to request " +
                    (requestingReadLock ? "read" : "write") +" lock on page " + pid);

            logReadingTransactions();
            logWritingTransactions();
            acquireStateLock();
            boolean hasReadLock = readingTransactions.containsKey(tid);
            boolean hasWriteLock = writingTransactions.containsKey(tid);
            // make sure one transaction only has at most one lock
            assert !(hasReadLock && hasWriteLock);
            if((requestingReadLock && hasReadLock) || ((!requestingReadLock) && hasWriteLock)){
                logger.log("already has the lock");
                releaseStateLock();
                return;
            }

//            if(hasWriteLock){
//                releaseStateLock();
//                throw new DbException("transaction " + tid + " is requesting a write lock but it already has the lock");
//            }

            // TODO: check if there is any transaction that needs to be aborted
            // check request redundant lock
//            if(hasReadLock && requestingReadLock){
//                releaseStateLock();
//                throw new DbException("transaction " + tid + " is requesting read lock but it already has the lock");
//            }
            // only 2 cases are allowed
            // 1. read lock -> write lock
            // 2. no lock   -> r/w lock
            releaseStateLock();
            try {
                if(perm == Permissions.READ_ONLY) { // read lock
                    logger.log("Trying to get read lock on page " + pid);
                    rwLock.readLock().lockInterruptibly();

                } else { // write lock
                    if(hasReadLock){ // if transaction already has the reading lock
                        logger.log("Already has the ready lock on " + pid +
                        ". Trying to release it first");
                        unlock(tid);
                    }
                    logger.log("Trying to get write lock on page " + pid);
                    rwLock.writeLock().lockInterruptibly();

                }
                // add the current lock request to the corresponding hash map
                acquireStateLock();
                TransactionThread tt = new TransactionThread(tid, Thread.currentThread());
                logger.log("put the new transaction thread object into the map");
                (perm == Permissions.READ_ONLY ? readingTransactions : writingTransactions).put(tid, tt);
                releaseStateLock();

            }catch (InterruptedException e){
                // if a thread is interrupted that means the transaction in this thread should abort
                throw new TransactionAbortedException();
            }
            logger.log("at the end end lock()");
            logReadingTransactions();
            logWritingTransactions();
            logger.log("end of lock()");
        }

        public boolean unlock(TransactionId tid)
                throws DbException {
            logger.log("In unlock() for transaction " + tid + " on page " + pid);
            logReadingTransactions();
            logWritingTransactions();
            acquireStateLock();
            boolean hasReadLock = readingTransactions.containsKey(tid);
            boolean hasWriteLock = writingTransactions.containsKey(tid);
            if(hasReadLock && hasWriteLock) {
                releaseStateLock();
                throw new DbException("How can " + tid + " have both r/w lock?");
            }
            if((!hasWriteLock) && (!hasReadLock)) {
                releaseStateLock();
                throw new DbException("No lock for " + tid + " on this page");
            }

            if(hasReadLock){
                rwLock.readLock().unlock();
                readingTransactions.remove(tid);

            }else{
                rwLock.writeLock().unlock();
                writingTransactions.remove(tid);
            }
            boolean hasLock = (readingTransactions.isEmpty() && writingTransactions.isEmpty());
            releaseStateLock();
            logReadingTransactions();
            logWritingTransactions();
            logger.log("end of unlock() for transaction " + tid + " on page " + pid);
            return hasLock;
        }

        public boolean holdsLock(TransactionId tid){
            acquireStateLock();
            boolean holdingLock = (readingTransactions.containsKey(tid)
                                || writingTransactions.containsKey(tid));
            releaseStateLock();
            return holdingLock;
        }

        private void logReadingTransactions(){
            logger.log("--Transactions holding read lock:--");
            for(TransactionId tid : readingTransactions.keySet())
                logger.log("transaction: " + tid);
            logger.log("--end--");
        }

        private void logWritingTransactions(){
            logger.log("--Transactions holding write lock:--");
            for(TransactionId tid : writingTransactions.keySet())
                logger.log("transaction: " + tid);
            logger.log("--end--");
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
