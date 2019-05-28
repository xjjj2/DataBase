package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private TupleDesc td;
    private RecordId rid;
    private List<Field> Fields=new ArrayList<>();
    public Tuple(TupleDesc td) {
        // some code goes here
    	this.td=td;
    	rid=null;
    	for (int i=0;i<td.numFields();++i) Fields.add(null);
    }
    @Override
    public boolean equals(Object o) {
    	if(!(o instanceof Tuple)) return false;
    	Tuple tp=(Tuple) o;
    	if (!td.equals(tp.td)) return false;
    	if (!rid.equals(tp.rid)) return false;
    	for (int i=0;i<Fields.size();++i) {
    		if (!Fields.get(i).compare(Predicate.Op.EQUALS, tp.Fields.get(i))) return false;
    	}
    	return true;
    }
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	Fields.set(i, f);
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	
        return Fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	String ans="";
    	for (int i=0;i<Fields.size();++i) {
    		ans+=Fields.get(i).toString();
    		if (i!=Fields.size()-1) ans+="\t";
    	}
    	return ans;
//        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Fields.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	this.td=td;
    	Fields=new ArrayList<>();
    	for (int i=0;i<td.numFields();++i) Fields.add(null);
        // some code goes here
    }
}
