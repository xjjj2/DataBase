package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tpdc;
    private int singlevalue;
    private Map<Field,Tuple> hashmap;
    private List<Tuple> retlist;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	hashmap=new HashMap<>();
    	if (gbfield!=NO_GROUPING) {
    		tpdc=new TupleDesc(new Type[] {gbfieldtype,Type.INT_TYPE},new String[] {null,null});
    	}
    	else {tpdc=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {null});}
        // some code goes here
    	if (what!=Op.COUNT) throw new IllegalArgumentException();
    	singlevalue=0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(gbfield==NO_GROUPING) {
    			singlevalue++;
    	}
    	else {
    		Field gp=tup.getField(gbfield);
    		if (!hashmap.containsKey(gp)) {
    			int value;
    			value=0;
    			Tuple tp=new Tuple(tpdc);
    			tp.setField(0, gp);
    			tp.setField(1, new IntField(value));
    			hashmap.put(gp, tp);
    		}
    		Tuple tp=hashmap.get(gp);
    		int value=((IntField)tp.getField(1)).getValue();
    		value++;
    		tp.setField(1, new IntField(value));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	if (gbfield==NO_GROUPING) {
    		retlist=new ArrayList<>();
    		Tuple tp=new Tuple(tpdc);
    		tp.setField(0, new IntField(singlevalue));
    		retlist.add(tp);
    	}
    	else {
    		retlist=new ArrayList<>(hashmap.values());
    	}
    	return new TupleIterator(tpdc,retlist);
    }

}
