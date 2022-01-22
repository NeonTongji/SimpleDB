package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
        public final Type fieldType;
        
        /**
         * The name of the field
         * 字段名称
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final Type[] types;
    private String[] names;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // done
        return new MyIterator();
    }

    class MyIterator implements Iterator<TDItem> {

        private int idx = -1;

        @Override
        public boolean hasNext() {
            return idx + 1 < types.length;
        }

        @Override
        public TDItem next() {
            idx++;
            if(hasNext()) {
                return new TDItem(types[idx], names[idx]);
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
        this.types = new Type[typeAr.length];
        for(int i = 0; i < typeAr.length; i++) types[i] = typeAr[i];
        this.names = new String[fieldAr.length];
        for(int i = 0; i < fieldAr.length; i++) names[i] = fieldAr[i];
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
        this.types = new Type[typeAr.length];
        for(int i = 0; i < typeAr.length; i++) types[i] = typeAr[i];
    }

    /**
     * @return the number of fields in this TupleDesc
     * 但是元组中字段的数量
     */
    public int numFields() {
        // done
        return types.length;
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
        if(this.names == null || i < 0 || i >= numFields())
            throw new NoSuchElementException("getFieldName失败，不存在该元素");
        return names[i];
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
        int len = numFields();
        if(i < len) {
            return types[i];
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

        if(name != null && this.names != null) {
            for(int i = 0; i < numFields(); i++) {
                if(name.equals(names[i])) return i;
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
        for(int i = 0; i < types.length; i++){
            size += types[i].getLen();
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
        Type[] types1 = td1.types;
        Type[] types2 = td2.types;
        Type[] type = new Type[types1.length + types2.length];
        System.arraycopy(types1, 0, type,0,types1.length);
        System.arraycopy(types2, 0,type,types1.length,types2.length);

        String[] names1 = td1.names;
        String[] names2 = td2.names;
        String[] name = new String[names1.length + names2.length];
        System.arraycopy(names1, 0, name,0,names1.length);
        System.arraycopy(names2, 0, name,names1.length,names2.length);

        TupleDesc tupleDesc = new TupleDesc(type, name);

        return tupleDesc;
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

    public boolean equals(Object o) {
        // done
        if(! (o instanceof  TupleDesc)) return false;

        if(this == null || this.types == null) return false;

        if(o instanceof TupleDesc) {

            if(this.numFields() != ((TupleDesc) o).numFields()) return false;

            for(int i = 0; i < types.length; i++) {
                if(!this.getFieldType(i).equals(((TupleDesc) o).getFieldType(i))) return  false;
            }
            if(this.names != null && ((TupleDesc) o).names != null) {
                for(int i = 0; i < names.length; i++) {
                    if(!this.getFieldName(i).equals(((TupleDesc) o).getFieldName(i))) return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        Objects.hash(this);
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

        for(int i = 0; i < types.length; i++) {
            if(types != null && names != null)
                sb.append(this.getFieldType(i) + "(" + this.getFieldName(i) +"), ");
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - 2) : "";
    }
}
