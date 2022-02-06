package simpledb.common;

import simpledb.transaction.TransactionId;

public class LockState {

    private TransactionId tid;
    private Permissions perm;

    public LockState(TransactionId tid, Permissions perm) {
        this.tid = tid;
        this.perm = perm;
    }

    public TransactionId getTid() {
        return tid;
    }

    public Permissions getPerm() {
        return perm;
    }

    @Override
    public int hashCode() {
        int result = tid.hashCode();
        result = 31 * result + perm.hashCode(); //hashCode采用奇质数，防止溢出，不会太散列，碰撞频率也不会太高
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        LockState another = (LockState) o;
        return tid.equals(another.tid) && perm.equals(another.perm);
    }
}
