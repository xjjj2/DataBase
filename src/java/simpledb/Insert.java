package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private DbFile table;
    private boolean called;
    private final TupleDesc tpdc=new TupleDesc(new Type[] {Type.INT_TYPE});
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
    	tid=t;
    	this.child=child;
    	this.tableid=tableId;
    	table=Database.getCatalog().getDatabaseFile(tableid);
    	if (!child.getTupleDesc().equals(table.getTupleDesc())) throw new DbException("Insert consturctor fail");
        // some code goes here
    	called=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tpdc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	super.close();
    	super.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (called)
    		return null;
    	called=true;
    	BufferPool bp=Database.getBufferPool();
    	int count=0;
    	while (child.hasNext()) {
    		Tuple tp=child.next();
    		++count;
    		try {
				bp.insertTuple(tid, tableid, tp);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	Tuple ret=new Tuple(tpdc);
    	ret.setField(0, new IntField(count));
    	return ret;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child=children[0];
    }
}
