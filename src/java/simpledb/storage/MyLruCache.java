//package simpledb.storage;
//
//import java.util.HashMap;
//import java.util.Iterator;
//
//public class MyLruCache<K, V> {
//
//    private HashMap<K, Node> cachedEntries;
//    private int capacity;
//    private Node head;
//    private Node tail;
//
//    public MyLruCache(int capacity) {
//        this.capacity = capacity;
//        cachedEntries = new HashMap<>(capacity);
//        head = new Node(null, null);
//        tail = new Node(null, null);
//        head.next = tail;
//        tail.prev = head;
//    }
//
//    public Iterator<V> iterator() {
//        return new MyLruIterator();
//    }
//
//    private class Node {
//        Node prev;
//        Node next;
//        K key;
//        V value;
//
//        Node(K key, V value) {
//            this.key = key;
//            this.value = value;
//        }
//    }
//
//    private void remove(Node node) {
//        node.prev.next = node.next;
//        node.next.prev = node.prev;
//    }
//
//    private void addFirst(Node node){
//        node.next = head.next;
//        node.next.prev = node;
//        head.next = node;
//        node.prev = head;
//    }
//
//    private void moveToFirst(Node node) {
//        remove(node);
//        addFirst(node);
//    }
//
//    private Node deleteOldest() {
//        Node last = tail.prev;
//        remove(last);
//        return last;
//    }
//
//    public V put(K key, V value) {
//        if(key == null || value == null) {
//            throw new IllegalArgumentException("不允许插入null");
//        }
//        if(isCached(key)){
//            Node node = cachedEntries.get(key);
//            node.value = value;
//            moveToFirst(node); //移到最前面
//            return null;
//        } else {
//            if(cachedEntries.size() == capacity) {
//                Node oldest = deleteOldest();
//                return oldest.value;
//            }
//            Node node = new Node(key, value);
//            addFirst(node);
//            cachedEntries.put(key, node);
//            return null;
//        }
//    }
//
//    public V get(K key) {
//        if(isCached(key)) {
//            Node node = cachedEntries.get(key);
//            moveToFirst(node);
//            return node.value;
//        }
//        return null;
//    }
//
//    private boolean isCached(K key) {
//        return cachedEntries.containsKey(key);
//    }
//
//    private class MyLruIterator implements Iterator<V> {
//
//        Node n = head;
//
//        @Override
//        public boolean hasNext() {
//            return n.next != null;
//        }
//
//        @Override
//        public V next() {
//            if(!hasNext()) {
//                throw new IllegalArgumentException("没有下一个");
//            }
//            Node next = n.next;
//            return next.value;
//        }
//    }
//}

package simpledb.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MyLruCache<K,V> {

    //存放当前缓存的条目
    protected HashMap<K,Node> cachedEntries;

    //允许缓存的最大条目数量
    protected int capacity;

    //头结点
    protected Node head;

    //最后一个结点
    protected Node tail;


    protected class Node{
        Node front;
        Node next;
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public MyLruCache(int capacity) {
        this.capacity = capacity;
        cachedEntries = new HashMap<>(capacity);
        head = new Node(null, null);
    }

    /**
     * 删除结点
     * @param ruNode the recently used Node
     */
    protected void unlink(Node ruNode) {
        //如果是最后一个结点
        if (ruNode.next == null) {
            ruNode.front.next = null;
        } else {
            ruNode.front.next=ruNode.next;
            ruNode.next.front=ruNode.front;
        }
    }

    /**
     * 把节点插入到表头作为第一个结点(在头结点之后)
     * @param ruNode  the recently used Node
     */
    protected void linkFirst(Node ruNode) {
        Node first= this.head.next;
        this.head.next=ruNode;
        ruNode.front= this.head;
        ruNode.next=first;
        if (first == null) {
            tail = ruNode;
        } else {
            first.front=ruNode;
        }
    }

    /**
     * 删除链表的最后一个元素
     * @return  返回被删除的元素
     */
    protected K removeTail() {
        K element=tail.key;
        Node newTail = tail.front;
        tail.front=null;
        newTail.next=null;
        tail=newTail;
        return element;
    }

    /**
     *
     * @param key
     * @param value
     * @return     被删除出缓存的条目，如果没有，返回null
     */
    public V put(K key, V value) {
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            //该结点存在于cache中，则更新其值，然后调整最近使用的条目，返回null(因为没有被删除的条目)
            Node ruNode = cachedEntries.get(key);
            ruNode.value=value;
            unlink(ruNode);
            linkFirst(ruNode);
            return null;
        } else  {
            //不存在的话先判断是否已经达到容量，是的话要先删除尾结点最后将其返回
            //还没有的话只需要新建结点，然后插入到表头，返回null
            V removed=null;
            if (cachedEntries.size() == capacity) {
                K removedKey=removeTail();
                removed = cachedEntries.remove(removedKey).value;
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cachedEntries.put(key, ruNode);
            return removed;
        }
    }

    /**
     *
     * @param key
     * @return  返回存在与缓存中的条目，不存在则返回null
     */
    public V get(K key) {
        if (isCached(key)) {
            //调整最近使用的条目
            Node ruNode = cachedEntries.get(key);
            unlink(ruNode);
            linkFirst(ruNode);
            return ruNode.value;
        }
        return null;
    }

    public boolean isCached(K key) {
        return cachedEntries.containsKey(key);
    }

    private void displayCache() {
        //用于测试的
        Node n=head;
        while ((n = n.next) != null) {
            System.out.print(n.value+", ");
        }
        System.out.println();
    }

    /**
     *
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    private class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public boolean hasNext() {
            return n.next!=null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n=n.next;
            return n.value;
        }
    }
}
