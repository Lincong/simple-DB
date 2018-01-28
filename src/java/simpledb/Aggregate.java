package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private Aggregator agg;
    private OpIterator chilOpIter;
    private OpIterator aggOpIter;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private TupleDesc resDesc;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        Type groupByFieldType = null;
        if(gfield != -1){ // has group by
            groupByFieldType = child.getTupleDesc().getFieldType(gfield);
        }

        if(aop == Aggregator.Op.COUNT)
            agg = new StringAggregator(gfield, groupByFieldType, afield, aop);
        else
            agg = new IntegerAggregator(gfield, groupByFieldType, afield, aop);

        // merge all tuple that can be read from the child OpIterator to the aggregator
        try {
            child.open();
            while(child.hasNext())
                agg.mergeTupleIntoGroup(child.next());

        }catch (DbException | TransactionAbortedException e){
            System.out.println("Oops...");
            e.printStackTrace();
            System.exit(1);
        }

        this.afield = afield;
        this.gfield = gfield;
        this.chilOpIter = child;
//        aggOpIter = agg.iterator();
        aggOpIter = null;
        this.aop = aop;
        // create tuple description of the current aggregate
        TupleDesc childDesc = child.getTupleDesc();
        Type [] types;
        String [] names;
        String aggFieldName = aop.toString() + "(" + childDesc.getFieldName(afield) + ")";
        if(gfield == Aggregator.NO_GROUPING){
            types = new Type[1];
            names = new String[1];
            types[0] = Type.INT_TYPE;
            names[0] = aggFieldName;

        } else {
            types = new Type[2];
            names = new String[2];
            types[0] = groupByFieldType;
            types[1] =  Type.INT_TYPE;
            names[0] = childDesc.getFieldName(gfield);
            names[1] = aggFieldName;
        }
        resDesc = new TupleDesc(types, names);
    }

    private void mergeAllTupsToAggregator(){
        // merge all tuple that can be read from the child OpIterator to the aggregator
        try {
            chilOpIter.open();
            while(chilOpIter.hasNext())
                agg.mergeTupleIntoGroup(chilOpIter.next());

        }catch (DbException | TransactionAbortedException e){
            System.out.println("Oops...");
            e.printStackTrace();
            System.exit(1);
        }
        aggOpIter = agg.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // some code goes here
	    return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // some code goes here
	    return chilOpIter.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    // some code goes here
	    return chilOpIter.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        mergeAllTupsToAggregator();
        aggOpIter.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        if(!aggOpIter.hasNext())
            return null;
	    return aggOpIter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        close();
        super.close();
        open();
        super.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
	    return resDesc;
    }

    public void close() {
	    // some code goes here
        chilOpIter.close();
        aggOpIter.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    // some code goes here
        OpIterator[] ret = new OpIterator[1];
        ret[0] = chilOpIter;
	    return ret;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // some code goes here
        assert children.length == 1;
        chilOpIter = children[0];
    }
}
