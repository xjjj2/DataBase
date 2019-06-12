package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private DbIterator child;
    private boolean called;
    private final TupleDesc tpdc=new TupleDesc(new Type[] {Type.INT_TYPE});
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	tid=t;
    	this.child=child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
				bp.deleteTuple(tid, tp);
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
