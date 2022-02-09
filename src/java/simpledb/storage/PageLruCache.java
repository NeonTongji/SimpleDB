package simpledb.storage;

import simpledb.common.Database;

import java.util.Iterator;

public class PageLruCache extends MyLruCache<PageId, Page>{

    public PageLruCache(int capacity) {
        super(capacity);
    }

    @Override
    public Page put(PageId key, Page value) {
        if(key == null || value == null) {
            throw new IllegalArgumentException("不允许插入null");
        }

        // 如果已经缓存了
        if(isCached(key)) {
            Node ruNode = cachedEntries.get(key); // protected修饰的cachedEntries, 子类也可以识别
            ruNode.value = value; // 重新赋值
            unlink(ruNode);
            linkFirst(ruNode);
            return null;
        } else { //没被缓存
            // 判断是否达到容量
            // 如果达到容量，判断尾结点是否dirty page， 如果是则取前一个page, 因为no steal 不能让dirty被刷到磁盘
            // 如果最后一个不是dirty page， 则直接将尾结点删除并返回
            // 未达到容量。新建page, 插到表头后返回
            Page removed = null;
            if(cachedEntries.size() == capacity) { //满了
                Page toRemove = null;
                Node n = tail;
                while((toRemove = n.value).isDirty() != null) {
                    n = n.front;
                    if(n == head) {
                        try {
                            throw new CacheException();
                        } catch (CacheException e) {
                            e.printStackTrace();
                        }
                    }
                }

                removePage(toRemove.getId());
                removed = cachedEntries.remove(toRemove.getId()).value;
            }

            Node node = new Node(key, value);
            linkFirst(node);
            cachedEntries.put(key, node);
            return removed;
        }
    }

    /**
     *
     * @param id
     */
    private void removePage(PageId id) {
       if(!isCached(id))  {
           throw new IllegalArgumentException();
       }
       Node toRemove = head;

       while(!(toRemove = toRemove.next).key.equals(id)) {
           if(toRemove == tail) {
               removeTail();
           } else {
               toRemove.next.front = toRemove.front;
               toRemove.front.next = toRemove.next;
           }
       }
    }

    /**
     * 恢复pid在磁盘中的状态
     * @param pid
     */
    public synchronized void reCachePage(PageId pid){
        if(!isCached(pid)) {
            throw new IllegalArgumentException();
        }

        //访问磁盘获得该page
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage origialPage = (HeapPage) table.readPage(pid); // 由于还没commit，瓷盘中的page还是clean的
        Node node = new Node(pid, origialPage); // 建立新的节点，放到LRU缓存中，替换dirtyPage
        cachedEntries.put(pid, node);
        Node toRemove = head;
        while(!(toRemove = toRemove.next).key.equals(pid));
        node.front = toRemove.front;
        node.next = toRemove.next;
        toRemove.front.next = node;
        if(toRemove.next != null) {
            toRemove.next.front = node;
        } else {
            tail = node;
        }
    }
}
