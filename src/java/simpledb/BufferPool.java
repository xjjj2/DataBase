package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private ReentrantLock tuplelock=new ReentrantLock(true);
    private ReentrantLock tuplelock2=new ReentrantLock(true);
    private ReentrantLock cyclelock=new ReentrantLock(true);
    public int abortcounter=0;
    public List<TransactionId> abortlist=new ArrayList<>();
    public List<TransactionId> commitlist=new ArrayList<>(); 
    private class pagelockwrapper{
    	public pagelockwrapper(PageId pid, boolean write) {
			super();
			this.pid = pid;
			this.write = write;
		}
		private PageId pid;
    	private boolean write;
    }
    private ConcurrentHashMap<TransactionId,pagelockwrapper> TransactionAcquiring=new ConcurrentHashMap<>();
    private class readwritelock{
    	private ReentrantLock writelock;
    	private Map<TransactionId,Boolean> readtid;
    	private TransactionId writetid;
    	private ReentrantLock LockGetting;
    	private AtomicInteger counter;
    	private PageId pid;
    	private AtomicInteger status; //1 true 0 false -1 null
    	private ReentrantLock WriteLockGetting;
    	private ReentrantLock WriteLockReleasing;
    	public readwritelock(PageId pid){
    		writelock=new ReentrantLock(true);
    		writetid=null;
    		LockGetting=new ReentrantLock(true);
    		counter=new AtomicInteger(0);
    		this.pid=pid;
     		readtid=new ConcurrentHashMap<>();
     		status=new AtomicInteger(-1);
     		WriteLockGetting=new ReentrantLock(true);
     		WriteLockReleasing=new ReentrantLock(true);
    	}
    	public ReentrantLock writeLock() {
    		return writelock;
    	}
    	public void acqforReadLock(TransactionId tid) throws TransactionAbortedException {
    		WriteLockReleasing.lock();
    		if (tid==writetid && status.get()==1) {WriteLockReleasing.unlock();return;}
    		WriteLockReleasing.unlock();
    		boolean temp2=BufferPool.this.cycleDetection(tid, pid, false);
			if (!temp2) {
				throw new TransactionAbortedException();
			}
    		while (true) {
    			LockGetting.lock();
    			boolean temp=BufferPool.this.cycleDetection(tid, pid, false);
    			if (!temp) {
    				LockGetting.unlock();
    				throw new TransactionAbortedException();
    			}
    			int n=0;
    			while (n<1000000000&&status.get()==1&&tid!=writetid) ++n;
    			writelock.lock();
    			if (status.get()==1&&tid!=writetid) {
    				writelock.unlock();
    				LockGetting.unlock();
    				continue;
    			}
    			if (status.get()!=-1 && readtid.containsKey(tid)) {
    				TransactionAcquiring.remove(tid);
    				writelock.unlock();
    				LockGetting.unlock();
    				break;
    			}
    			if (!(status.get()==1)) {
    				if (counter.incrementAndGet()==1 && tid!=writetid) {writetid=tid;}
    				status.set(0);
    			}
    			readtid.put(tid,true);
    			TransactionAcquiring.remove(tid);
    			writelock.unlock();
    			LockGetting.unlock();
    			break;
    		}
    	}
    	public void acqforWriteLock(TransactionId tid) throws TransactionAbortedException {
//    		if (tid==writetid) return;
    		// TODO acquire a writelock when readlock is only acquiried by this transaction may cause a bug
    		WriteLockReleasing.lock();
    		if (tid==writetid && status.get()==1) {WriteLockReleasing.unlock();return;}
    		WriteLockReleasing.unlock();
    		LockGetting.lock();
    		if (tid==writetid && counter.get()==1) {status.set(1);counter.set(0);readtid.remove(tid);LockGetting.unlock();return;}
    		LockGetting.unlock();
    		boolean temp2=BufferPool.this.cycleDetection(tid, pid, true);
			if (!temp2) {
				throw new TransactionAbortedException();
			}
    		while (true) {
    			WriteLockGetting.lock();
    			boolean temp=BufferPool.this.cycleDetection(tid, pid, true);
    			if (!temp) {
    				WriteLockGetting.unlock();
    				throw new TransactionAbortedException();
    			}
    			int n=0;
    			while (n<1000000000&&status.get()!=-1&&(status.get()==1&&tid!=writetid||status.get()==0&&!(tid==writetid&&counter.get()==1))) ++n;
    /*			if (n>=1000000000){
    				WriteLockGetting.unlock();
    				throw new TransactionAbortedException();
    			}*/
    			writelock.lock();
    			if (status.get()!=-1&&(status.get()==1&&tid!=writetid||status.get()==0&&!(tid==writetid&&counter.get()==1))) {
    				writelock.unlock();
    				WriteLockGetting.unlock();
    				continue;
    			}
    			if (status.get()==-1) {
    				status.set(1);
    			}
    			else {
    				status.set(1);
    				counter.set(0);
    				readtid.remove(tid);
    			}
    			TransactionAcquiring.remove(tid);
    			writetid=tid;
    			writelock.unlock();
    			WriteLockGetting.unlock();
    			break;
    		}
    	}
    	public void releaseReadLock(TransactionId tid) {
    		LockGetting.lock();
    		readtid.remove(tid);
    		if (counter.decrementAndGet()==0) {
    			writetid=null;
    			status.set(-1);
    		}
    		else if (counter.get()==1) {
    			for (TransactionId td:readtid.keySet())
    				writetid=td;
    		}
     		LockGetting.unlock();
    	}
    	public void releaseWriteLock(TransactionId tid) {
    		WriteLockReleasing.lock();
    		writetid=null;
    		status.set(-1);
    		WriteLockReleasing.unlock();
    	}
    	public void clear(TransactionId tid) {
    		writetid=null;
    		counter.set(0);
    		status.set(-1);
    	}
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    
    private boolean cycleDetection(TransactionId tid, PageId pid, boolean write) {
    	cyclelock.lock();
    	TransactionAcquiring.put(tid, new pagelockwrapper(pid,write));
    	boolean temp=false;
    	int depth = 0;
    	try {
    		temp=DetectionDfs(tid,tid,depth);
    	}
    	catch (Throwable e){
    		temp=false;
    	}
    	cyclelock.unlock();
    	return temp;
    }
    private boolean DetectionDfs(TransactionId nowtid,TransactionId tid,int depth) throws Throwable {
    	if (nowtid==null) return true;
    	depth++;
    	if (depth>=1000) throw new Throwable();
    	if (!TransactionAcquiring.containsKey(nowtid)) return true;
    	pagelockwrapper pl=TransactionAcquiring.get(nowtid);
    	PageId pid=pl.pid;
    	boolean write=pl.write;
    	readwritelock lock=lockmap.get(pid);
    	if (write) {
    		if (lock.status.get()==-1) return true;
    		if (lock.status.get()==1) {
    			if (nowtid==lock.writetid) return true;
    			if (tid==lock.writetid) return false;
    			return DetectionDfs(lock.writetid,tid,depth);
    		}
    		else {
    			for (TransactionId tranid:lock.readtid.keySet()) {
    				if (nowtid==tranid) continue;
    				if (tranid==tid) return false;
    				if (!DetectionDfs(tranid,tid,depth)) return false;
    			}
    		}
    	}
    	else {
    		if (lock.status.get()==-1) return true;
    		if (lock.status.get()==1) {
    			if (nowtid==lock.writetid) return true;
    			if (tid!=lock.writetid) {
    				return DetectionDfs(lock.writetid,tid,depth+1);
    			}
    			else return false;
    		}
    	}
    	return true;
    }
    private class bufferunit{
    	Page page;
    	int pin;
    	boolean dirty;
    	Lock lock;
    	public bufferunit() {
    		pin=0;
    		dirty=false;
    	}
    	public bufferunit(Page pg) {
    		pin=0;
    		dirty=false;
    		page=pg;
    	}
    }
    private ConcurrentHashMap<PageId,bufferunit> buffer;
    private ConcurrentHashMap<PageId,readwritelock> lockmap;
    private ConcurrentHashMap<TransactionId,Map<PageId,Boolean>> transhold;
    private int pagenum;
    public BufferPool(int numPages) {
        // some code goes here
    	buffer=new ConcurrentHashMap<>();
    	pagenum=numPages;
    	lockmap=new ConcurrentHashMap<>();
    	transhold=new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	synchronized (this) {if (!lockmap.containsKey(pid)) {
    		lockmap.put(pid, new readwritelock(pid));
    	}
    	}
    	readwritelock lock=lockmap.get(pid);
    	if (perm==perm.READ_ONLY) lock.acqforReadLock(tid); 
    	else lock.acqforWriteLock(tid);
    	if (!transhold.containsKey(tid)) transhold.put(tid, new ConcurrentHashMap<>());
    	transhold.get(tid).put(pid,perm==perm.READ_WRITE);
    	Page ret;
    	Catalog cata=Database.getCatalog();
    	if (buffer.containsKey(pid)) {
    		 bufferunit unit=buffer.get(pid);
    		 ret=unit.page;
    	}
    	else {
    		if (buffer.size()<pagenum) {
    			DbFile db=cata.getDatabaseFile(pid.getTableId());
    			Page pg=db.readPage(pid);
    			buffer.put(pid, new bufferunit(pg));
    			ret=pg;
    		}
    		else {
    			boolean ev=evictPage();
    			// to evict page
    			DbFile db=cata.getDatabaseFile(pid.getTableId());
    			Page pg=db.readPage(pid);
    			buffer.put(pid, new bufferunit(pg));
    			ret=pg;
    		}
    	}
/*    	if (perm==perm.READ_ONLY) lock.readLock().unlock();
    	else lock.writeLock().unlock();*/
    	return ret;
        // some code goes here
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	if (!lockmap.containsKey(pid)) return;
    	readwritelock lock=lockmap.get(pid);
    	lock.clear(tid);
    	// some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	commitlist.add(tid);
		flushPages(tid);
    	TransactionAcquiring.remove(tid);
    	if (transhold.containsKey(tid))
        for (PageId pid:transhold.get(tid).keySet()) {
        	readwritelock lock=lockmap.get(pid);
        	if (lock.readtid.containsKey(tid)) lock.releaseReadLock(tid);
        	if (lock.counter.get()==0&&lock.writetid==tid) lock.releaseWriteLock(tid);
        }
    	 transhold.remove(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	if (!transhold.containsKey(tid)) return false;
        return transhold.get(tid).containsKey(p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	if (commit) {
    		transactionComplete(tid);
    	}
    	else {
    		++abortcounter;
    		abortlist.add(tid);
    		for (PageId pid:transhold.get(tid).keySet()) {
    			discardPage(pid);
    		}
    		TransactionAcquiring.remove(tid);
        	if (transhold.containsKey(tid))
            for (PageId pid:transhold.get(tid).keySet()) {
            	readwritelock lock=lockmap.get(pid);
            	if (lock.readtid.containsKey(tid)) lock.releaseReadLock(tid);
            	if (lock.counter.get()==0&&lock.writetid==tid) lock.releaseWriteLock(tid);
            }
        	 transhold.remove(tid);
    	}
       
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> dirty=Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	for (Page pg:dirty) {
    		if (!buffer.containsKey(pg.getId())) {
 //   			Database.getCatalog().getDatabaseFile(pg.getId().getTableId()).writePage(pg);
    			if (pagenum<=buffer.size()) evictPage();
    			buffer.put(pg.getId(), new bufferunit(pg));
    			buffer.get(pg.getId()).dirty=true;pg.markDirty(true, tid);
    			/*if (buffer.size()<pagenum) {
        			buffer.put(pg.getId(), new bufferunit(pg));
        		}
        		else {
        			evictPage();
        			// to evict page
        			buffer.put(pg.getId(), new bufferunit(pg));
        		}*/
    		}
    		else {buffer.get(pg.getId()).dirty=true;pg.markDirty(true, tid);}
    	}
    	
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> dirty=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    	for (Page pg:dirty) {
    		if (!buffer.containsKey(pg.getId())) {
//    			Database.getCatalog().getDatabaseFile(pg.getId().getTableId()).writePage(pg);
    			if (pagenum<=buffer.size()) evictPage();
    			buffer.put(pg.getId(), new bufferunit(pg));
    			buffer.get(pg.getId()).dirty=true;pg.markDirty(true, tid);
    			/*if (buffer.size()<pagenum) {
        			buffer.put(pg.getId(), new bufferunit(pg));
        		}
        		else {
        			evictPage();
        			// to evict page
        			buffer.put(pg.getId(), new bufferunit(pg));
        		}*/
    		}
    		else {buffer.get(pg.getId()).dirty=true;pg.markDirty(true, tid);}
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pid:buffer.keySet()) {
    		flushPage(pid);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (!buffer.containsKey(pid)) return;
        bufferunit unit=buffer.get(pid);
        unit.page.markDirty(false, null);
    	if (buffer.containsKey(pid)) buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	if (!buffer.containsKey(pid)) return;
    	bufferunit unit=buffer.get(pid);
    	if (unit.dirty || unit.page.isDirty()!=null) {
    		Page pg=unit.page;
    		Database.getCatalog().getDatabaseFile(pg.getId().getTableId()).writePage(pg);
    		pg.markDirty(false, null);
    	}
    	unit.dirty=false;
        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
    	if (!transhold.containsKey(tid)) return;
    	for (PageId pgid:transhold.get(tid).keySet()) {
    		flushPage(pgid);
    	}
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  boolean evictPage() throws DbException {
    	// if fail to evict a page return false
    	PageId pidar[]=new PageId[pagenum];
    	buffer.keySet().toArray(pidar);
    	
    	int rn=(int)(Math.random()*pagenum);
    	try {
    		for (int i=rn;i<rn+pagenum;++i) {
    			PageId pgid=pidar[i % pagenum];
    			bufferunit unit=buffer.get(pgid);
    			if (unit.dirty||unit.page.isDirty()!=null) continue;
    			flushPage(pidar[i%pagenum]);
    			buffer.remove(pidar[i%pagenum]);
    			return true;
    		}
		} catch (IOException e) {
			throw new DbException("error when evict page");
		}

    	throw new DbException("no page to evict");
        // some code goes here
        // not necessary for lab1
    }

}
