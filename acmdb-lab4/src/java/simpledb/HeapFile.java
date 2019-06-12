package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	file=f;
    	this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return file.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pagesize=BufferPool.getPageSize();
    	int offset=pid.pageNumber()*pagesize;
    	byte[] data=new byte[pagesize];
    	try {
			RandomAccessFile reader=new RandomAccessFile(file, "r");
			reader.seek(offset);
			reader.read(data);
			reader.close();
			return new HeapPage((HeapPageId) pid,data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IllegalArgumentException();
		}
    	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	RandomAccessFile rf=new RandomAccessFile(file,"rw");
    	rf.seek(page.getId().pageNumber()*BufferPool.getPageSize());
    	rf.write(page.getPageData());
    	rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	return (int) ((file.length()+BufferPool.getPageSize()-1)/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	BufferPool bp=Database.getBufferPool();
    	ArrayList<Page> dirty=new ArrayList<>();
    	for (int i=0;i<numPages();++i) {
    		HeapPage pg=(HeapPage) bp.getPage(tid, new HeapPageId(getId(),i), Permissions.READ_WRITE);
    		if (pg.getNumEmptySlots()!=0) {
    			pg.insertTuple(t);
    			dirty.add(pg);
    			pg.markDirty(true, tid);
    			return dirty;
    		}
    	}
    	HeapPageId pid=new HeapPageId(getId(),numPages());
    	RandomAccessFile rf = new RandomAccessFile(file, "rw");
    	rf.seek(BufferPool.getPageSize()*numPages());
    	rf.write(new byte[BufferPool.getPageSize()]);
    	rf.close();
    	Database.getBufferPool().discardPage(pid);
    	HeapPage pg=(HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
    	pg.insertTuple(t);
    	dirty.add(pg);
		pg.markDirty(true, tid);
        return dirty;
        // not necessary for lab1
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	BufferPool bp=Database.getBufferPool();
    	if (t.getRecordId().getPageId().getTableId()!=this.getId()) throw new DbException("error from heapfile delete tuple");
    	ArrayList<Page> dirty=new ArrayList<>();
    	HeapPage pg=(HeapPage) bp.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	pg.deleteTuple(t);
    	dirty.add(pg);
    	pg.markDirty(true, tid);
        return dirty;
        // not necessary for lab1
    }
    public class HeapFileIterator implements DbFileIterator{

    	private boolean status;
    	private File file;
    	private int pgNo;
    	private Tuple nextTuple;
    	private Iterator<Tuple> nowit;
    	private BufferPool buffer;
    	private TransactionId tid;
    	private HeapFileIterator(File file,TransactionId tid) {
    		this.file=file;
    		status=false;
    		buffer=Database.getBufferPool();
    		this.tid=tid;
    		pgNo=0;
    		nowit=null;	
    	}
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			status=true;
			if (nowit==null)
				getNext();
		}
		private void getNext() throws TransactionAbortedException, DbException {
			// TODO Auto-generated method stub
			if (nowit==null) {
				HeapPage page=(HeapPage) buffer.getPage(tid, new HeapPageId(getId(),pgNo), Permissions.READ_WRITE);
				nowit=page.iterator();
			}
			while (!nowit.hasNext() && pgNo<numPages()) {
				++pgNo;
				if (pgNo==numPages()) break;
				HeapPage page=(HeapPage) buffer.getPage(tid, new HeapPageId(getId(),pgNo), Permissions.READ_WRITE);
				nowit=page.iterator();
			}
			if (pgNo==numPages()) nextTuple=null;
			else nextTuple=nowit.next();
		}
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (!status) return false;
			return nextTuple!=null;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if (nextTuple==null || !status) throw new NoSuchElementException();
			Tuple Temp=nextTuple;
			getNext();
			return Temp;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (!status) throw new DbException(null);
			pgNo=0;
			nextTuple=null;
			nowit=null;
			getNext();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			status=false;
		}
    	
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(file,tid);
    }

}

