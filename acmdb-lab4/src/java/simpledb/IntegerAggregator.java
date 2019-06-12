package simpledb;

import java.util.*;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tpdc;
    private int singlevalue;
    private int singlecount;
    private Map<Field,Tuple> hashmap;
    private Map<Field,Integer> hashcount;
    private List<Tuple> retlist;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	hashmap=new HashMap<>();
    	hashcount=new HashMap<>();
    	if (gbfield!=NO_GROUPING) {
    		tpdc=new TupleDesc(new Type[] {gbfieldtype,Type.INT_TYPE},new String[] {null,null});
    	}
    	else {tpdc=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {null});}
    	if(what==Op.MAX) {
    		singlevalue=-2147483648;
    	}
    	else if (what==Op.MIN) {
    		singlevalue=2147483647;
    	}
    	else singlevalue=0;
    	singlecount=0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if(gbfield==NO_GROUPING) {
    		if (what==Op.COUNT) {
    			singlevalue++;
    		}
    		else if (what==Op.AVG || what==Op.SUM) {
    			singlevalue+=((IntField)tup.getField(afield)).getValue();
    		}
    		else if (what==Op.MAX){
    			int x=((IntField)tup.getField(afield)).getValue();
    			if (x>singlevalue) singlevalue=x;
    		}
    		else if (what==Op.MIN) {
    			int x=((IntField)tup.getField(afield)).getValue();
    			if (x<singlevalue) singlevalue=x;
    		}
    		singlecount++;
    	}
    	else {
    		Field gp=tup.getField(gbfield);
    		if (!hashmap.containsKey(gp)) {
    			int value;
    			if(what==Op.MAX) {
    	    		value=-2147483648;
    	    	}
    	    	else if (what==Op.MIN) {
    	    		value=2147483647;
    	    	}
    	    	else value=0;
    			Tuple tp=new Tuple(tpdc);
    			tp.setField(0, gp);
    			tp.setField(1, new IntField(value));
    			hashmap.put(gp, tp);
    			hashcount.put(gp, 0);
    		}
    		Tuple tp=hashmap.get(gp);
    		int value=((IntField)tp.getField(1)).getValue();
    		Integer ct=hashcount.get(gp);
    		++ct;
    		hashcount.put(gp, ct);
    		if (what==Op.AVG || what==Op.SUM) {
    			value+=((IntField)tup.getField(afield)).getValue();
    		}
    		else if (what==Op.COUNT) {
    			value++;
    		}
    		else if (what==Op.MAX){
    			int x=((IntField)tup.getField(afield)).getValue();
    			if (x>value) value=x;
    		}
    		else if (what==Op.MIN) {
    			int x=((IntField)tup.getField(afield)).getValue();
    			if (x<value) value=x;
    		}
    		tp.setField(1, new IntField(value));
    	}
        // some code goes here
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	if (gbfield==NO_GROUPING) {
    		retlist=new ArrayList<>();
    		Tuple tp=new Tuple(tpdc);
    		if (what==Op.AVG)
    			tp.setField(0, new IntField(singlevalue/singlecount));
    		else tp.setField(0, new IntField(singlevalue));
    		retlist.add(tp);
    	}
    	else {
    		if (what==Op.AVG) {
    			retlist=new ArrayList<>();
    			for (Entry<Field,Tuple> entry:hashmap.entrySet()) {
    				Field gp=entry.getKey();
    				int count=hashcount.get(gp);
    				Tuple tp=entry.getValue();
    				Tuple ad=new Tuple(tpdc);
    				ad.setField(0, gp);
    				ad.setField(1, new IntField(((IntField)tp.getField(1)).getValue()/count));
    				retlist.add(ad);
    			}
    		}
    		else retlist=new ArrayList<>(hashmap.values());
    	}
    	return new TupleIterator(tpdc,retlist);
        /*// some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab3");*/
    }

}
