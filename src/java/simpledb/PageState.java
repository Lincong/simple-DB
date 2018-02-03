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
                        unlock(tid);
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

        public boolean unlock(TransactionId tid)
                throws DbException {

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
            return hasLock;
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
