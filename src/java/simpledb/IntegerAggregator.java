package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Aggregator.Op op;
    private boolean isDescSet;
    private Tuple noGbRes;
    private int noGbCnt;
    private int noGbSum;
    private Map<Object, Tuple> groups;
    private Map<Object, Integer> groupByFieldCnt;
    private Map<Object, Integer> groupByFieldSum;
    private TupleDesc resDesc;
    private DbLogger logger = new DbLogger(getClass().getName(), getClass().getName() + ".log",false);
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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        op = what;
        isDescSet = false;
    }

    private Tuple getZeroCountTuple(Tuple tup){
        Tuple t = new Tuple(resDesc);
        Field zeroCounterField = new IntField(0);
        if(gbfield == NO_GROUPING) {
            t.setField(0, zeroCounterField);
        } else { // if it is group-by, the first field in the tuple is the group by field and the second
            t.setField(1, zeroCounterField);
            t.setField(0, tup.getField(gbfield));
        }
        return t;
    }

    private void setTupleFieldInt(Tuple tup, int aggFieldVal){
        int fieldIdx = (gbfield == NO_GROUPING ? 0 : 1);
        Field updatedCounterField = new IntField(aggFieldVal);
        tup.setField(fieldIdx, updatedCounterField);
    }

    private void setResTupleDesc(Tuple tup){
        // create the schema (description) for the tuple
        Type[] tdTypes;
        String[] tdNames;
        String afieldName = op.toString() + "(" + tup.getField(afield).toString() + ")";

        if (gbfield == NO_GROUPING){
            assert gbfieldtype == null;
            tdTypes = new Type[1];
            tdNames = new String[1];
            tdTypes[0] = Type.INT_TYPE;
            tdNames[0] = afieldName;
            resDesc = new TupleDesc(tdTypes, tdNames);
            noGbRes = getZeroCountTuple(tup);
            noGbCnt = 0;
            noGbSum = 0;

        } else {
            tdTypes = new Type[2];
            tdNames = new String[2];
            tdTypes[0] = gbfieldtype;
            tdTypes[1] = Type.INT_TYPE;
            tdNames[0] = tup.getTupleDesc().getFieldName(gbfield);
            tdNames[1] = afieldName;

            resDesc = new TupleDesc(tdTypes, tdNames);
            groups = new HashMap<>();
            groupByFieldCnt = new HashMap<>();
            groupByFieldSum = new HashMap<>();
        }
    }

    private int getTupleAggFieldInt(Tuple tup){
//        int idx = (gbfield == NO_GROUPING ? 0 : 1);
        return ((IntField) tup.getField(afield)).getValue();
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(!isDescSet){
            setResTupleDesc(tup);
            isDescSet = true;
        }

//        logger.log("-----Op: " + op.toString());
//        logger.log("Tuple to merge: " + tup.toString());
        int anotherTupleAggFieldInt = getTupleAggFieldInt(tup);
        int currTupleAggFieldInt;
        Object key = null;
        if(gbfield != NO_GROUPING){
            Field groupByField = tup.getField(gbfield);
            if(gbfieldtype == Type.INT_TYPE)
                key = ((IntField) groupByField).getValue();
            else // STRING_TYPE
                key = ((StringField) groupByField).getValue();

            if(!groups.containsKey(key)){
                Tuple t = getZeroCountTuple(tup);
                groups.put(key, t);
                groupByFieldCnt.put(key, 0);
                groupByFieldSum.put(key, 0);
                currTupleAggFieldInt = 0;

            } else {
                currTupleAggFieldInt = ((IntField) groups.get(key).getField(1)).getValue();
            }
        }else{
            currTupleAggFieldInt = ((IntField) noGbRes.getField(0)).getValue();
        }

        int aggFieldVal;
        Tuple updateTuple;
        int mergedTupleCnt;
        switch (op) {
            case MAX:
                aggFieldVal = Math.max(anotherTupleAggFieldInt, currTupleAggFieldInt);
                updateTuple = (gbfield == NO_GROUPING ? noGbRes : groups.get(key));
                setTupleFieldInt(updateTuple, aggFieldVal);
                break;

            case MIN:
                mergedTupleCnt = (gbfield == NO_GROUPING ? noGbCnt: groupByFieldCnt.get(key));
                if(mergedTupleCnt == 0)
                    aggFieldVal = anotherTupleAggFieldInt;
                else
                    aggFieldVal = Math.min(anotherTupleAggFieldInt, currTupleAggFieldInt);

                updateTuple = (gbfield == NO_GROUPING ? noGbRes : groups.get(key));
                setTupleFieldInt(updateTuple, aggFieldVal);
                break;

            case SUM:
                aggFieldVal = currTupleAggFieldInt + anotherTupleAggFieldInt;
                updateTuple = (gbfield == NO_GROUPING ? noGbRes : groups.get(key));
                setTupleFieldInt(updateTuple, aggFieldVal);
                break;

            case AVG:
                int sum;
                mergedTupleCnt = (gbfield == NO_GROUPING ? noGbCnt: groupByFieldCnt.get(key));
                sum = (gbfield == NO_GROUPING ? noGbSum : groupByFieldSum.get(key)) + anotherTupleAggFieldInt;
                if (gbfield == NO_GROUPING)
                    noGbSum = sum;
                else
                    groupByFieldSum.put(key, sum);

                aggFieldVal = sum / (mergedTupleCnt + 1);
                updateTuple = (gbfield == NO_GROUPING ? noGbRes : groups.get(key));
                setTupleFieldInt(updateTuple, aggFieldVal);
                break;

            default:
                break;
        }
        if(op == Op.MIN || op == Op.AVG) {
            if (gbfield == NO_GROUPING)
                noGbCnt++;
            else
                groupByFieldCnt.put(key, groupByFieldCnt.get(key) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tups = new LinkedList<>();
        if(gbfield == NO_GROUPING){
            tups.add(noGbRes);
        }else{
            for(Object key : groups.keySet())
                tups.add(groups.get(key));
        }
        logger.log("-----Op: " + op.toString());
        logger.log("Tuples in the iterable:");
        logger.log(tups.toString());
        logger.log("\n");
        return new TupleIterator(resDesc, tups);
    }
}
