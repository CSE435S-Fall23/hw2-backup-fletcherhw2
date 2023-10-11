package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 *
 * Student: Ben Fletcher 498067
 */
public class Aggregator {
	
	private AggregateOperator memOperator;
	private boolean memGroupBy;
	private TupleDesc memTupleDesc;
	private ArrayList<Tuple> resultAggregateTuples;

	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		
		memOperator = o;
		memGroupBy = groupBy;
		memTupleDesc = td;
		
		Tuple initTuple = new Tuple(td);
		
		resultAggregateTuples = new ArrayList<Tuple>();
		resultAggregateTuples.add(initTuple);

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		
		if(this.memGroupBy) {
			mergeGroupBy(t);
		}
		else {
			mergeWithoutGroupBy(t);
		}	
	}
	
	
	
	//split the group by and non group by aggregate code into two seperate helper methods 
	//for readability
	private void mergeGroupBy(Tuple t) {
		switch(memOperator) {
		
			case MAX:
	
			case MIN:
	
			case AVG:
	
			case COUNT:
	
			case SUM:
			
		
		}
	}
	private void mergeWithoutGroupBy(Tuple t) {
		
		switch(memOperator) {
		
			case MAX:
			
				//if the current max of all the tuples is less than the merged tuples value, then new tuple
				//value is the max
				if(resultAggregateTuples.get(0).getField(0).compare(RelationalOperator.LT, t.getField(0))) {
					resultAggregateTuples.set(0, t);
				}
				
			case MIN:
				
				//if the current min of all the tuples is greater than the merged tuples value, then new tuple
				//value is the min
				if(resultAggregateTuples.get(0).getField(0).compare(RelationalOperator.GT, t.getField(0))) {
					resultAggregateTuples.set(0, t);
				}
				
			case AVG:
				
			//should there be an avg for strings?
			//any other way than to cast here?
				
			//times the old average by the number of tuples to get the actual value
			int tupleVal = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString()) * resultAggregateTuples.size();
			//calculate new average  by adding new tuple's value and divide by the new num of tuples
			int newAverage = tupleVal + Integer.parseInt(t.getField(0).toString()) / (resultAggregateTuples.size() + 1);
			
			Tuple newTupleAVG = new Tuple(memTupleDesc);
			newTupleAVG.setField(0, new IntField(newAverage));
			
			resultAggregateTuples.set(0,newTupleAVG);
			
				
				
			case COUNT:
				//casting also used here
				
				//every time tuple is merged just add one to pre-existing count
				int newCount = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString()) + 1;
				Tuple newTupleCount = new Tuple(memTupleDesc);
				newTupleCount.setField(0, new IntField(newCount));
				
				resultAggregateTuples.set(0,newTupleCount);
				
				
			case SUM:
				
				//every time tuple is merged just add curr tuples value to pre-existing sum
				int oldSum = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString());
				int newSum = oldSum + Integer.parseInt(t.getField(0).toString());
				
				Tuple newTupleSum = new Tuple(memTupleDesc);
				newTupleSum.setField(0, new IntField(newSum));
				
				resultAggregateTuples.set(0,newTupleSum);
		
		}
	}
	
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		return resultAggregateTuples;
	}

}
