package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    private TransactionId lastDirtyOperation;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here

        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        return (int) Math.ceil(getNumTuples() / 8.0);
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        // 如果slotId对应的槽为空
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // 根据元组的recordId可以获得其所在页的id，元组的编号
        RecordId tRecordId = t.getRecordId();
        HeapPageId tPid = (HeapPageId) tRecordId.getPageId();
        int tNo = tRecordId.getTupleNumber();
        if(!tPid.equals(pid) || !isSlotUsed(tNo)) {
            throw new DbException("要删除的元组本来就为空");
        }
        tuples[tNo] = null;
        markSlotUsed(tNo, false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1

        if(!t.getTupleDesc().equals(td)) {
            throw new DbException("TD匹配不上");
        }


        for(int i = 0; i < getNumTuples(); i++) {
            if(!isSlotUsed(i)) {
                tuples[i] = t;
                // 重要：每个tuple都有一个recordId, 记得新增了要给他一个身份证
                t.setRecordId(new RecordId(pid, i));
                markSlotUsed(i, true);
                return;
            }
        }
        throw new DbException("这个Page满了");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        lastDirtyOperation = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return lastDirtyOperation;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int num = 0;
        for(int i = 0; i < numSlots; i++) {
            if(!isSlotUsed(i)) num++;
        }
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // 若不为8的倍数的槽是满的，18个slots, 则 byte[] header 为[11111111, 11111111, 00000011]
        // 按照文档说明。每个字节的最低位代表第一个槽的状态
        int byteNum = i / 8; // 计算在第几个字节
        int bitNum = i % 8; // 计算为从右往左第几个比特，即第几高位
//        return (header[byteNum] & (1 << bitNum)) == 1; // 注意 不是等于1，可能等于1 2 4 8等等 如 11和 10与
        return (header[byteNum] & (1 << bitNum)) > 0;
//        return 0 != (header[i >> 3] & (1 << (i & 7)));
    }

    /**
     * 将某个slot的值改为1或0
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int byteNum = i / 8;//计算在第几个字节
        int posInByte = i % 8;//计算在该字节的第几位,从右往左算（这是因为JVM用big-ending）
        header[byteNum] = editBitInByte(header[byteNum], posInByte, value);
    }

    /**
     * 修改一个byte的指定位置的bit
     * @param target    待修改的byte
     * @param posInByte bit的位置在target的偏移量，从右往左且从0开始算，取值范围为0到7
     * @param value     为true修改该bit为1,为false时修改为0
     * @return 修改后的byte
     */
    private byte editBitInByte(byte target, int posInByte, boolean value) {
        if (posInByte < 0 || posInByte > 7) {
            throw new IllegalArgumentException();
        }
        byte b = (byte) (1 << posInByte);//将1这个bit移到指定位置，例如pos为3,value为true，将得到00001000
        //如果value为1,使用字节00001000以及"|"操作可以将指定位置改为1，其他位置不变
        //如果value为0,使用字节11110111以及"&"操作可以将指定位置改为0，其他位置不变
        // 非运算符是又得讲一下的，理解起来很容易的，就是按位取反，比如~8对吧，
        // 那就是00001000按位取反结果是11110111.
        // value: true, 则将 0 改为1，将 0000 0 111 | 0000 1 0000 = 0000 1111
        // value: false, 则将 1 改为0, 将 0000 1 111 & 0000 1 0000 ？？ 这样不行
        //                        而是 0000 1 1111 & (1111 0 1111) = 0000 0 1111
        return value ? (byte) (target | b) : (byte) (target & ~b);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new Iterator<Tuple>() {
            private int idx = 0;
            private int cntUsed = 0;
            private int used = getNumTuples() - getNumEmptySlots();

            @Override
            public boolean hasNext() {
                return idx  < getNumTuples() && cntUsed < used;
            }

            @Override
            public Tuple next() {
                if(!hasNext()) throw new NoSuchElementException("迭代越界");
                while(!isSlotUsed(idx)) idx++;
                cntUsed++;
                return tuples[idx++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("tuple不允许删除");
            }
        };
    }

}

