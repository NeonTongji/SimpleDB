package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tdAr;
    private int numFields;

    /**
     * A help class to facilitate organizing the information of each field
     * TDItem是组织每个字段信息的辅助类
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * 字段类型
         * */
        public Type fieldType;

        /**
         * The name of the field
         * 字段名称
         * */
        public String fieldName;


        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TDItem) {
                TDItem another = (TDItem) o;
                return Objects.equals(fieldName, another.fieldName) && Objects.equals(fieldType, another.fieldType);
            } else return false;
        }


        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }



    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // done
        return new MyIterator();
    }

    private class MyIterator implements Iterator<TDItem> {

        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < tdAr.length;
        }

        @Override
        public TDItem next() {

            if(hasNext()) {
                return tdAr[idx++];
            } else {
                throw new NoSuchElementException("迭代数组越界");
            }
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 根据类型数组 和 字段名称 数组 创建元组描述
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // done
        if(typeAr.length == 0) {
            throw new IllegalArgumentException("类型数组为空");
        }
        if(fieldAr.length != typeAr.length) {
            throw new IllegalArgumentException("字段数组filedAr长度必须和类型数组tyAr长度一致");
        }

        numFields = typeAr.length;
        this.tdAr = new TDItem[numFields];

        for(int i = 0; i < numFields; i++) {
            tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // done;
        if(typeAr.length == 0) {
            throw new IllegalArgumentException("类型数组为空");
        }

        numFields = typeAr.length;
        this.tdAr = new TDItem[numFields];

        for(int i = 0; i < numFields; i++) {
            tdAr[i] = new TDItem(typeAr[i], "");
        }

    }

    private TupleDesc(TDItem[] tdItems) {
        if(tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("tdItems数组不合法");
        }
        this.tdAr = tdItems;
    }

    /**
     * @return the number of fields in this TupleDesc
     * 但是元组中字段的数量
     */
    public int numFields() {
        // done
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // done
        if(i < 0 || i >= numFields())
            throw new NoSuchElementException("getFieldName失败，不存在该元素");
        return tdAr[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // done
        if(i >=0 &&i < numFields()) {
            return tdAr[i].fieldType;
        } else {
            throw new NoSuchElementException("数组越界");
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // done

        if(name == null) {
            throw new NoSuchElementException();
        }

        for(int i = 0; i < tdAr.length; i++) {
            if(tdAr[i].fieldName != null && tdAr[i].fieldName.equals(name)) {
                return i;
            }
        }

        throw new NoSuchElementException("没有这个字段名");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     *         元组中所有类型的长度
     */
    public int getSize() {
        // done

        int size = 0;
        for(int i = 0; i < tdAr.length; i++){
            size += tdAr[i].fieldType.getLen();
        }
        return size;

    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // done
        TDItem[] tdAr1 = td1.tdAr;
        TDItem[] tdAr2 = td2.tdAr;
        int len1 = tdAr1.length;
        int len2 = tdAr2.length;
        TDItem[] tdItems = new TDItem[len1 + len2];
        System.arraycopy(tdAr1, 0, tdItems,0,len1);
        System.arraycopy(tdAr2, 0, tdItems, len1, len2);
        return new TupleDesc(tdItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

//    public boolean equals(Object o) {
//        // done
//        if(this == o) return true;
//
//        if(!(o instanceof TupleDesc)) return false;
//
//        TupleDesc anotherTd = (TupleDesc) o;
//
//        if(numFields() != anotherTd.numFields()) return false;
//
//        for(int i = 0; i < numFields(); i++) {
//            if(!tdAr[i].equals(anotherTd.tdAr[i])) {
//                return false;
//            }
//        }
//        return true;
//    }

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            //当且仅当field数量相同且每个的field的名字和类型都相同时返回true
            TupleDesc another = (TupleDesc) o;
            if (!(another.numFields() == this.numFields())) {
                return false;
            }
            for (int i = 0; i < numFields(); i++) {
                if (!tdAr[i].equals(another.tdAr[i])) {
                    return false;
                }
            }
            return true;
        } else return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        return Objects.hash(this);
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < tdAr.length; i++) {
                sb.append(this.getFieldType(i) + "(" + this.getFieldName(i) +"), ");
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 2) : "";
    }
}


