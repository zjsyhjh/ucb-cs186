package simpledb;

import java.security.acl.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;



public class LockManager {

    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
        exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    }

    public synchronized boolean hasSharedLock(PageId pid, TransactionId tid) {
        return sharedLocks.get(pid) != null && sharedLocks.get(pid).contains(tid);
    }

    public synchronized boolean hasExclusiveLock(PageId pid, TransactionId tid) {
        return exclusiveLocks.get(pid) != null && exclusiveLocks.get(pid).equals(tid);
    }

    public synchronized boolean getLock(Permissions perm, TransactionId tid, PageId pid) {
        if (Permissions.READ_ONLY.equals(perm)) {
            if (hasSharedLock(pid, tid) || hasExclusiveLock(pid, tid)) {
                return true;
            }
            if (exclusiveLocks.get(pid) == null) {
                if (sharedLocks.get(pid) == null) {
                    sharedLocks.put(pid, new ArrayList<TransactionId>());
                }
                sharedLocks.get(pid).add(tid);
                return true;
            }
            return false;
        } else if (Permissions.READ_WRITE.equals(perm)) {
            if (hasExclusiveLock(pid, tid)) {
                return true;
            }
            if (sharedLocks.get(pid) != null && sharedLocks.get(pid).contains(tid) && sharedLocks.get(pid).size() == 1) {
                exclusiveLocks.put(pid, tid);
                sharedLocks.get(pid).remove(tid);
                return true;
            }
            if (exclusiveLocks.get(pid) == null && sharedLocks.get(pid) == null) {
                exclusiveLocks.put(pid, tid);
                return true;
            }
            return false;
        }
        return false;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (exclusiveLocks.get(pid) == null && sharedLocks.get(pid) == null) {
            return false;
        }
        if (sharedLocks.get(pid) != null) {
            sharedLocks.get(pid).remove(tid);
        }
        if (exclusiveLocks.get(pid) != null) {
            exclusiveLocks.remove(pid);
        }
        return true;
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        for (PageId pid : sharedLocks.keySet()) {
            if (sharedLocks.get(pid).contains(tid)) {
                releaseLock(tid, pid);
            }
        }
        for (PageId pid : exclusiveLocks.keySet()) {
            if (exclusiveLocks.get(pid).equals(tid)) {
                releaseLock(tid, pid);
            }
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        boolean s = sharedLocks.get(pid).contains(tid);
        boolean e = exclusiveLocks.get(pid).equals(tid);
        return s || e;
    }
}
