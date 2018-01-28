package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Map<Object, Tuple> groups;
    private TupleDesc resDesc;
    private Tuple noGbRes;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT)
            throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        // create the schema (description) for the tuple
        Type[] tdTypes;
        String[] tdNames;
        if (gbfield == NO_GROUPING){
            assert gbfieldtype == null;
            tdTypes = new Type[1];
            tdNames = new String[1];
            tdTypes[0] = Type.INT_TYPE;
            tdNames[0] = what.toString();
            TupleDesc resDesc = new TupleDesc(tdTypes, tdNames);
            noGbRes = new Tuple(resDesc);
            noGbRes.setField(0, );

        } else {
            tdTypes = new Type[2];
            tdNames = new String[2];
            tdTypes[0] = gbfieldtype;
            TupleDesc resDesc = new TupleDesc(tdTypes, tdNames);
            groups = new HashMap<Object, Tuple>();

        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield == NO_GROUPING){
            int currCnt = noGbRes.
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
