package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    // Key： Page资源， List: 存放了事务id和锁类型， 代表在page上加的锁
    private Map<PageId, List<LockState>> lockStateMap;

    //Key: 事务， PageId，正在等待的资源，
    //BufferPool中采用sleep体现等待
    private Map<TransactionId, PageId> waitingInfo;

    public LockManager() {
        lockStateMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }

    //=======================申请锁， 加锁， 解锁===========================

    /**
     * 共享锁
     * 如果tid在pid上有读锁，返回true
     * 如果tid在pid上有写锁，或者没有锁而且tid可以给pid加读锁，则加锁后返回true
     * 如果tid不能给pid加读锁，返回false
     * @param tid
     * @param pid
     * @return 返回tid能否在pid上加锁
     */
    public synchronized boolean grantSLock(TransactionId tid, PageId pid){
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStateMap.get(pid);

        if(lockStates != null && lockStates.size() != 0) {
            if(lockStates.size() == 1) { //pid上只有一个锁
                Iterator<LockState> iterator = lockStates.iterator();
                LockState lockState = iterator.next();
                if(lockState.getTid().equals(tid)){ // 通过事务id判断是否为本事务的锁
                    // 如果是自己锁：读锁直接返回，写锁则加锁后返回
                    return lockState.getPerm() == Permissions.READ_ONLY || lock(pid, tid, Permissions.READ_ONLY);
                } else {
                    // 如果是别人的读锁，则加锁后再返回; 如果是别人的写锁，陷入阻塞队列
                    return lockState.getPerm() == Permissions.READ_ONLY ? lock(pid, tid, Permissions.READ_ONLY) :
                            wait(tid, pid);
                }
            } else { // pid有多个锁
                // 1. 读锁 + 写锁，都为tid的锁
                // 2. 读锁 + 写锁，都不是tid的锁
                // 3. 读锁 + 读锁，一个是tid的一个不是pid的
                // 4. 读锁 + 读锁， 都没有tid的锁
                for(LockState lockState : lockStates) {
                    if(lockState.getPerm() == Permissions.READ_WRITE) { //代表有写锁，判断写锁是否为自己的
                        // 是自己的直接返回，不是自己的阻塞
                        return lockState.getTid().equals(tid) || wait(tid, pid);
                    } else if(lockState.getTid().equals(tid)) {
                        // 有自己的读锁
                        return true;
                    }
                }
                // 遍历完后都没有自己的读锁和写锁，则加个写锁后返回
                return lock(pid, tid, Permissions.READ_ONLY);
            }
        } else {
            return lock(pid, tid, Permissions.READ_ONLY);
        }
    }

    /**
     * 排他锁
     * 若tid在pid上已经有写锁，则返回true
     * 若只有tid拥有pid的读锁，或者tid在pid上没有锁，但tid可以给pid加写锁，则加锁后返回true
     * 若tid此时不能给pid加写锁，返回false
     * @param tid
     * @param pid
     * @return 返回tid能否持有pid的排他锁
     */
    public synchronized boolean grantXLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStateMap.get(pid);
        if(lockStates != null && lockStates.size() == 0) {
            if(lockStates.size() == 1) { // 若pid上只有一个锁
                LockState lockState = lockStates.get(0);
                //如果是自己的写锁，直接返回
                //如果是自己的读锁，加写锁后返回
                //如果是别的事务的锁，则必须等待
                return lockState.getTid().equals(tid) ? lockState.getPerm() == Permissions.READ_WRITE || lock(pid,
                        tid,Permissions.READ_WRITE) : wait(tid, pid);
            } else {
                // 有多个锁
                // 1. 两个锁都属于tid，读锁 + 写锁 , 返回ture
                // 2. 两个锁都不属于tid，读锁 + 写锁, 需要wait
                // 3. 多个读锁，需要wait
                if(lockStates.size() == 2) {
                    for(LockState lockState : lockStates) {
                        if(lockState.getTid().equals(tid) && lockState.getPerm() == Permissions.READ_WRITE) {
                            return true;
                        }
                    }
                }

                return wait(tid, pid);
            }
        } else { //pid上没有锁
            return lock(pid, tid, Permissions.READ_WRITE);
        }
    }

    private synchronized boolean wait(TransactionId tid, PageId pid) {
        waitingInfo.put(tid, pid);
        return false;
    }

    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        LockState lockState = new LockState(tid, perm);
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStateMap.get(pid);
        if(lockStates == null) {
            lockStates = new ArrayList<>();
        }
        lockStates.add(lockState);
        lockStateMap.put(pid, lockStates);
        waitingInfo.remove(tid);
        return true;
    }

    /**
     * 解锁，将pid对应的lockstates list中锁消除
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean unlock(TransactionId tid, PageId pid) {
        ArrayList<LockState> lockStates = (ArrayList<LockState>) lockStateMap.get(pid);

        if(lockStates == null || lockStates.size() == 0) return false;
        LockState lockState = getLockState(tid, pid);
        if(lockState == null) return false;
        lockStates.remove(lockState);
        lockStateMap.put(pid, lockStates);
        return true;
    }


    /**
     * 释放tid所有的资源
     * @param tid
     */
    public synchronized void releaseTransactionLocks(TransactionId tid) {
        List<PageId> toRelease = getAllLockedPagesById(tid);
        for(PageId pid : toRelease) {
            unlock(tid, pid);
        }
    }

    // ========================================查询与修改 页面锁状态map 和 事务等待资源map======================

    /**
     *
     * @param tid
     * @param pid
     * @return 返回当前页面pid的tid持有的锁状态
     */
    public synchronized LockState getLockState(TransactionId tid, PageId pid) {
        List<LockState> lockStates = lockStateMap.get(pid);
        if(lockStates == null || lockStates.size() == 0) return null;
        for(LockState lockState : lockStates) {
            if(lockState.getTid().equals(tid)) {
                return lockState;
            }
        }
        return null;
    }

    /**
     *
     * @param tid
     * @return 返回tid持有的所有页面
     */
    public synchronized List<PageId> getAllLockedPagesById(TransactionId tid) {
        List<PageId> pids = new ArrayList<>();
        for(Map.Entry<PageId, List<LockState>> entry : lockStateMap.entrySet()) {
            for(LockState lockState : entry.getValue()) {
                if(lockState.getTid().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

    // ========================================查询与修改两个map信息方法======================

    //========================检查死锁====================================
    /**
     * 通过检测资源的依赖图根据是否存在环来判断是否已经陷入死锁
     * 具体实现：本事务tid需要检测“正在等待的资源的拥有者是否已经直接或间接的在等待本事务tid已经拥有的资源”
     * <p>
     * 如图，括号内P1,P2,P3为资源,T1,T2,T3为事务
     * 虚线以及其上的字母R加上箭头组成了拥有关系，如果是字母W则代表正在等待写锁
     * 例如下图左上方T1到P1的一连串符号表示的是T1此时拥有P1的读锁
     * 图的边缘可以是虚线的转折点，例如为了表示T2正在等待P1
     * <p>
     * //   ---T1---R-->P1<-------
     * //                       W
     * //  ----------------------
     * //  W
     * //  ---T2---R-->P2<-------
     * //                       W
     * //  ----------------------
     * //  W
     * //  ---T3---R-->P3
     * <p>
     * 上图的含义是，Ti拥有了对Pi的读锁(1<=i<=3)
     * 因为T1在P1上有了读锁，所以T2正在等待P1的写锁
     * 同理，T3正在等待P2的写锁
     * <p>
     * 现在假设的情景是，此时T1要申请对P3的写锁，进入等待，这将会造成死锁
     * 而接下来调用这个方法判断，就可以得知已经产生死锁从而回滚事务（具体在BufferPool的getPage()方法的while循环开始处）
     * <p>
     * 导致死锁的本质原因就是将等待的资源(P3)的拥有者(T3)间接的在等待T1拥有的资源(P1)
     * 下面方法的注释以这个例子为基础，具体解释这个方法是如何判断出“T1在P3上的等待已经造成了死锁”的
     * @param tid
     * @param pid
     * @return true: tid陷入死锁，false: 没有陷入死锁
     */
    public synchronized boolean deadLockOccurred(TransactionId tid, PageId pid) {
        List<LockState> lockStates = lockStateMap.get(pid); // 当前页面的锁状态
        // 没有锁，当然不会陷入死锁
        if(lockStates == null || lockStates.size() == 0) {
            return false;
        }

        List<PageId> pagesLockedBytid = getAllLockedPagesById(tid); // tid锁住的资源
        for(LockState lockState : lockStates) {
            TransactionId holder = lockState.getTid(); // pid页面锁的持有者
            if(!holder.equals(tid)) { // pid不是被tid锁住，而是被另一个holder锁住
                // 当前页面锁持有者holder 想请求 tid 锁住的资源 pagesLockedBytid， 是否需要等待
                // 需要等待代表： holder请求tid锁住的，tid想拥有holder当前锁住的，即发生了死锁
                boolean isWaiting = isWaitingResources(holder, pagesLockedBytid, tid);
                if(isWaiting) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     *
     * @param holder 当前事务需要资源的持有者
     * @param pageIds 当前事务持有的资源
     * @param tid 当前事务
     * @return holder是否需要等待 tid锁住的 pageIds, 等待：true, 不等待： false
     *
     * <>
     *     直接等待
     *     tid ---------> cur(holder)
     *     holder------->pageIds(tid)
     *     holder----waiting------>waitingPage
     *     如果pageIds 中包含 waitingPage，代表死锁
     *
     *     间接等待
     *                                         _______ holder1  ------> hold by tid?  ________ 直接等待
     *                                       /                                       / yes
     *     holder----waiting------->waitingPage  _______ holder2  ------> hold by tid?
     *                                       \                                      \ no
     *                                         ------- holder3 ------> hold by tid?   看看 holder1、2、3的持有者是不是tid
     *                                                                                      ↓
     *                                                           递归     iswaitingResources(holder1, pageIds, tid)
     * </>
     */
    private boolean isWaitingResources(TransactionId holder, List<PageId> pageIds, TransactionId tid) {
        PageId waitingPage = waitingInfo.get(holder); // holder正在等待的资源

        if(waitingPage == null) { // holder不需要等待资源，返回fasle
            return false;
        }

        for(PageId pageId: pageIds) {
            if(pageId.equals(waitingPage)) {
                // 如果holder正在等待的资源，恰巧是tid锁住的资源
                // holder 请求tid 锁住的资源
                // tid 请求 holder锁住的页面
                // 则陷入死锁
                return true;
            }
        }

        // 到达这里说明holder并不直接等待pageIds中的任意一个page, 但不代表holder不在等待，有可能是间接在等待
        // 如果waitingPage的持有者中的某一个正在等待holder锁住的pagesIds中的一个，说明holder是在间接等待
        List<LockState> waitedByHolder = lockStateMap.get(waitingPage); // hoder等待资源的锁状态
        if(waitedByHolder == null || waitedByHolder.size() == 0) return false; // holder等待的资源非常自由，不需要等待

        for(LockState lockState : waitedByHolder) {
            TransactionId childHolder = lockState.getTid(); // holder等待资源的持有者,
            if(!childHolder.equals(tid)) { // 如果持有者不是tid
                boolean isWaiting = isWaitingResources(childHolder, pageIds, tid); // 判断
                if(isWaiting) return true;
            }
        }

        return false;
    }

    // ==========================================检测死锁 end ============================================

}
