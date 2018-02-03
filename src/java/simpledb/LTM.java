package simpledb;
/**
 * Created by lincongli on 2/2/18.
 */

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

// a k-v map: PID -> page state
// page state (only one thread is allowed to r/w the page state at a time):
    // 1. a list of transactions holding read lock on the page
    // 2. a list of transactions holding write lock on the page
    //
public class LTM {

    ConcurrentMap<PageId, PageState> lockTable;
    Semaphore stateLock;

    public LTM(){
        lockTable = new ConcurrentHashMap<>();
        stateLock = new Semaphore(1, true);
    }

    public void getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        PageState ps;
        acquireStateLock();
        if(!lockTable.containsKey(pid)){
            ps = new PageState();
            lockTable.put(pid, ps);
        }else{
            ps = lockTable.get(pid);
        }
        releaseStateLock();
        ps.lock(tid, perm);
    }

    public void returnLock(TransactionId tid, PageId pid)
            throws DbException {
        acquireStateLock();
        if(!lockTable.containsKey(pid)){
            releaseStateLock();
            throw new DbException("No lock for page " + pid);
        }
        PageState ps = lockTable.get(pid);
        releaseStateLock();
        if(ps.unlock(tid)){ // if there is no lock on this page
            acquireStateLock();
            lockTable.remove(pid);
            releaseStateLock();
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        boolean ret;
        acquireStateLock();
        if (!lockTable.containsKey(pid)){
            ret = false;
        }else{
            ret = lockTable.get(pid).holdsLock(tid);
        }
        releaseStateLock();
        return ret;
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
