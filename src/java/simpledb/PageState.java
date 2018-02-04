package simpledb;

/**
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

class PageState {


        // allows one transaction to hold both read and write lock
        class RWlock{

            Semaphore permits;
            Semaphore syncPermit;
            int totalPermitsNum = Integer.MAX_VALUE;
            boolean holdsRWLocks;

            public RWlock(){
                permits = new Semaphore(totalPermitsNum, true);
                syncPermit = new Semaphore(1, true);
                holdsRWLocks = false;
            }

            public void getReadLock() throws InterruptedException {
                getReadLock(false);
            }

            public void getReadLock(boolean holdWriteLock) throws InterruptedException {
                start();
                if(holdWriteLock){
                    holdsRWLocks = true;
                    end();
                } else {
                    end();
                    permits.acquire();
                }
            }
            // upgrade from read lock to write lock and assuming we already have the read lock
            public void upgradeLock() throws InterruptedException {
                permits.acquire(Integer.MAX_VALUE-1);
                holdsRWLocks = true;
            }

            public void returnReadLock() {
                startUninterruptable();
                if(holdsRWLocks){
                    holdsRWLocks = false;

                } else {
                    permits.release();
                }
                end();
            }

            // if the read lock is already held. upgradeLock() should be called instead
            public void getWriteLock() throws InterruptedException {
                permits.acquire(Integer.MAX_VALUE);
                holdsRWLocks = true;
            }

            public void returnWriteLock() {
                startUninterruptable();
                if(holdsRWLocks) {
                    permits.release(Integer.MAX_VALUE - 1);
                    holdsRWLocks = false;

                }else{
                    permits.release(Integer.MAX_VALUE);
                }
                end();
            }

            private void start() throws InterruptedException {
                syncPermit.acquire();
            }

            private void startUninterruptable() {
                syncPermit.acquireUninterruptibly();
            }

            private void end(){
                syncPermit.release();
            }
        }

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

            public void logThread(){
                logger.log("thread: " + thread);
            }
        }

        private Map<TransactionId, TransactionThread> readingTransactions;
        private Map<TransactionId, TransactionThread> writingTransactions;
        private Semaphore stateLock;
        private RWlock rwLock;
        private PageId pid;
        private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log", false);

        PageState(PageId pid){
            readingTransactions = new HashMap<>();
            writingTransactions = new HashMap<>();
            stateLock = new Semaphore(1, true);
            rwLock = new RWlock();
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
            if((requestingReadLock && hasReadLock) || ((!requestingReadLock) && hasWriteLock)){
                logger.log("already has the lock");
                releaseStateLock();
                return;
            }

            // TODO: check if there is any transaction that needs to be aborted

            // only 2 cases are allowed
            // 1. read lock -> write lock
            // 2. no lock   -> r/w lock
            releaseStateLock();
            try {
                if(perm == Permissions.READ_ONLY) { // read lock
                    logger.log("Trying to get read lock on page " + pid);
                    if(hasWriteLock){ // transaction already has write lock
                        rwLock.getReadLock(true);
                    }else {
                        rwLock.getReadLock();
                    }
                } else { // write lock
                    if(hasReadLock){ // if transaction already has the reading lock
                        logger.log("Already has the ready lock on " + pid +
                        ". Trying to upgrade it");
                        rwLock.upgradeLock();
                    } else {
                        logger.log("Trying to get write lock on page " + pid);
                        rwLock.getWriteLock();
                    }
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
            logger.log("hasReadLock: " + hasReadLock);
            boolean hasWriteLock = writingTransactions.containsKey(tid);
            logger.log("hasWriteLock: " + hasWriteLock);
            logger.log("current thread: " + Thread.currentThread());

            if((!hasWriteLock) && (!hasReadLock)) {
                releaseStateLock();
                throw new DbException("No lock for " + tid + " on this page");
            }
            if(hasReadLock){
                logger.log("Trying to unlock read lock");
                rwLock.returnReadLock();
                readingTransactions.remove(tid);
            }

            if(hasWriteLock){
                logger.log("Trying to unlock write lock");
                rwLock.returnWriteLock();
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
            for(TransactionId tid : readingTransactions.keySet()) {
                logger.log("transaction: " + tid);
                readingTransactions.get(tid).logThread();
            }
            logger.log("--end--");
        }

        private void logWritingTransactions(){
            logger.log("--Transactions holding write lock:--");
            for(TransactionId tid : writingTransactions.keySet()) {
                logger.log("transaction: " + tid);
                writingTransactions.get(tid).logThread();
            }
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
