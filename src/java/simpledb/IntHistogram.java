package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int buckets;
	private int min;
	private int max;
	private int bucket[];
	private int total;
	private double bucketsize;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets=buckets;
    	this.max=max;
    	this.min=min;
    	bucket=new int[buckets];
    	bucketsize=((double)(max-min))/buckets;
    }
    private int getBucket(int v) {
//    	int x=(v-min)*buckets/(max-min);
    	long x=((long)(v-min))*buckets/(max-min);
    	if (v==max) --x;
    	return (int)x;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	bucket[getBucket(v)]++;
    	++total;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	if (op==Predicate.Op.EQUALS) {
    		if (getBucket(v)<0 || getBucket(v)>=buckets) return 0;
    		return (1/bucketsize*bucket[getBucket(v)]/total);
    	}
    	if (op==Predicate.Op.NOT_EQUALS) {
    		return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
    	}
    	if (op==Predicate.Op.GREATER_THAN_OR_EQ) {
    		if (getBucket(v)<0) return 1;
    		if (getBucket(v)>=buckets) return 0;
    		int i=getBucket(v);
    		if (i<0) i=0;
    		if (i>=buckets) i=buckets-1;
    		double t=0;
    		t+=min+(i+1)*bucketsize-v;
    		t*=bucket[i];
    		t/=bucketsize;
    		for (int j=i+1;j<buckets;++j) {
    			t+=bucket[j];
    		}
    		return t/total;
    	}
    	if (op==Predicate.Op.GREATER_THAN)
    		return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v)-estimateSelectivity(Predicate.Op.EQUALS, v);
    	if (op==Predicate.Op.LESS_THAN_OR_EQ) {
    	/*	int i=getBucket(v);
    		if (i<0) i=0;
    		if (i>=buckets) i=buckets-1;
    		double t=0;
    		t+=v-min-i*bucketsize;
    		t*=bucket[i];
    		t/=bucketsize;
    		for (int j=i-1;j>=0;--j) {
    			t+=bucket[j];
    		}
    		return t/total;*/
    		return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
    	}
    	if (op==Predicate.Op.LESS_THAN)
    		return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v)-estimateSelectivity(Predicate.Op.EQUALS, v);
    	// some code goes here
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
    	
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
