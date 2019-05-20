package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private DbIterator child;
    private Predicate p;
    private TupleDesc tpdc;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p=p;
    	this.child=child;
    	getDesc();
    	
    }
    private void getDesc() {
    	tpdc=child.getTupleDesc();
    }
    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tpdc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
    	child.close();
    	super.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	super.close();
    	super.open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while (child.hasNext()){
    		Tuple tp=child.next();
    		if (p.filter(tp)) return tp;
    	}
    	return null;
    	
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator x[]=new DbIterator[1];
    	x[0]=child;
        return x;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child=children[0];
    	getDesc();
    }

}
