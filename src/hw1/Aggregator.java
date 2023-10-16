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
	private int count;
	
	private ArrayList<Tuple> resultAggregateTuples;
	//used to keep track of unique group names for a group by aggregator
	private ArrayList<String> groupByNames;

	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		
		memOperator = o;
		memGroupBy = groupBy;
		memTupleDesc = td;
		
		//to help with average calculations
		count = 0;
		
		resultAggregateTuples = new ArrayList<Tuple>();
		
		groupByNames = new ArrayList<String>();

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
		
		count++;
	}
	
	
	
	//split the group by and non group by aggregate code into two seperate helper methods 
	//for readability
	
	private void mergeGroupBy(Tuple t) {
		switch(memOperator) {
		
			case MAX:
				//check to see if this tuple's group has already been accounted for or not
				if( !groupByNames.contains(t.getField(0).toString()) ) {
					groupByNames.add(t.getField(0).toString());
					
					Tuple aggTupleGB = new Tuple(memTupleDesc);
					//set the first field to the group name
					//second field will be the grouped value
					aggTupleGB.setField(0, new StringField(t.getField(0).toString()));
					aggTupleGB.setField(1,t.getField(1));
					resultAggregateTuples.add(aggTupleGB);
				}
				
				//this group has already been visited, so check if its the max
				else {
					String name = t.getField(0).toString();
					
					//find right tuple for this group
					for(Tuple tup: resultAggregateTuples) {
						
						Field tupget = tup.getField(0);
						Field tget = new StringField(t.getField(0).toString());
						if(tupget.compare(RelationalOperator.EQ, tget)== true) {
							
							if(tup.getField(1).compare(RelationalOperator.LT, t.getField(1))==true) {
								
								tup.setField(1, t.getField(1));
							}
						}
					}
				}
				

	
			case MIN:
				
				//check to see if this tuple's group has already been accounted for or not
				if( !groupByNames.contains(t.getField(0).toString()) ) {
					groupByNames.add(t.getField(0).toString());
					
					Tuple aggTupleGB = new Tuple(memTupleDesc);
					//set the first field to the group name
					//second field will be the grouped value
					aggTupleGB.setField(0, new StringField(t.getField(0).toString()));
					aggTupleGB.setField(1,t.getField(1));
					resultAggregateTuples.add(aggTupleGB);
				}
				
				//this group has already been visited, so check if its the min
				else {
					
					String name = t.getField(0).toString();
					
					//find right tuple for this group
					for(Tuple tup: resultAggregateTuples) {
						
						Field tupget = tup.getField(0);
						Field tget = new StringField(t.getField(0).toString());
						if(tupget.compare(RelationalOperator.EQ, tget)== true) {
							
							if(tup.getField(1).compare(RelationalOperator.GT, t.getField(1))==true) {
								
								tup.setField(1, t.getField(1));
							}
						}
					}
				}
	
			case AVG:
				//check to see if this tuple's group has already been accounted for or not
				if( !groupByNames.contains(t.getField(0).toString()) ) {
					groupByNames.add(t.getField(0).toString());
					
					Tuple aggTupleGB = new Tuple(memTupleDesc);
					//set the first field to the group name
					//second field will be the grouped value
					aggTupleGB.setField(0, new StringField(t.getField(0).toString()));
					aggTupleGB.setField(1, t.getField(1));
					resultAggregateTuples.add(aggTupleGB);
				}
				
				//this group has already been visited, so re-adjust the avg
				else {
					String name = t.getField(0).toString();
					
					//find right tuple for this group
					for(Tuple tup: resultAggregateTuples) {
						
						Field tupget = tup.getField(0);
						Field tget = new StringField(t.getField(0).toString());
						if(tupget.compare(RelationalOperator.EQ, tget)== true) {
							
							int oldAvg = Integer.parseInt(tup.getField(1).toString());
							//recalculate average
							int newAvg = ((oldAvg * count) + Integer.parseInt(t.getField(1).toString()) ) / (count +1);

							tup.setField(1, new IntField(newAvg));
						}
					}
				}
	
			case COUNT:
				
				//check to see if this tuple's group has already been accounted for or not
				if( !groupByNames.contains(t.getField(0).toString()) ) {
					groupByNames.add(t.getField(0).toString());
					
					Tuple aggTupleGB = new Tuple(memTupleDesc);
					//set the first field to the group name
					//second field will be the grouped value
					aggTupleGB.setField(0, new StringField(t.getField(0).toString()));
					aggTupleGB.setField(1, new IntField(Integer.MIN_VALUE));
					resultAggregateTuples.add(aggTupleGB);
				}
				
				//this group has already been visited, so re-adjust the count
				else {
					String name = t.getField(0).toString();
					
					//find right tuple for this group
					for(Tuple tup: resultAggregateTuples) {
						
						Field tupget = tup.getField(0);
						Field tget = new StringField(t.getField(0).toString());
						if(tupget.compare(RelationalOperator.EQ, tget)== true) {
							
							int oldCount = Integer.parseInt(tup.getField(1).toString());
							//recalculate average
							int newCount = oldCount + 1;

							tup.setField(1, new IntField(newCount));
						}
					}
				}
	
			case SUM:
				
				//check to see if this tuple's group has already been accounted for or not
				if( !groupByNames.contains(t.getField(0).toString()) ) {
					groupByNames.add(t.getField(0).toString());
					
					Tuple aggTupleGB = new Tuple(memTupleDesc);
					//set the first field to the group name
					//second field will be the grouped value
					aggTupleGB.setField(0, new StringField(t.getField(0).toString()));
					aggTupleGB.setField(1, t.getField(1));
					resultAggregateTuples.add(aggTupleGB);
				}
				
				//this group has already been visited, so re-adjust the count
				else {
					String name = t.getField(0).toString();
					
					//find right tuple for this group
					for(Tuple tup: resultAggregateTuples) {
						
						Field tupget = tup.getField(0);
						Field tget = new StringField(t.getField(0).toString());
						if(tupget.compare(RelationalOperator.EQ, tget)== true) {
							
							int oldSum = Integer.parseInt(tup.getField(1).toString());
							//recalculate average
							int newSum = oldSum + Integer.parseInt(t.getField(1).toString());

							tup.setField(1, new IntField(newSum));
							
						}
					}
				}
	
		
		}
	}
	private void mergeWithoutGroupBy(Tuple t) {
		
		switch(memOperator) {
		
			case MAX:
				
				//handling if current tuple is the first tuple being merged
				if(resultAggregateTuples.size()==0) {
					
					Tuple aggTup = new Tuple(memTupleDesc);
					aggTup.setField(0, t.getField(0));
					resultAggregateTuples.add(aggTup);
				}
				//first tuple has been handled
				else {
					//if the current max of all the tuples is less than the merged tuples value, then new tuple
					//value is the max
					if(resultAggregateTuples.get(0).getField(0).compare(RelationalOperator.LT, t.getField(0))) {
						resultAggregateTuples.set(0, t);
					}
				}
			

				
			case MIN:
				
				//handling if current tuple is the first tuple being merged
				if(resultAggregateTuples.size()==0) {
					
					Tuple aggTup = new Tuple(memTupleDesc);
					aggTup.setField(0, t.getField(0));
					resultAggregateTuples.add(aggTup);
				}
				//first tuple has been handled
				else {
					//if the current min of all the tuples is greater than the merged tuples value, then new tuple
					//value is the min
					if(resultAggregateTuples.get(0).getField(0).compare(RelationalOperator.GT, t.getField(0))) {
						resultAggregateTuples.set(0, t);
					}			
				}

	
		//should there be an avg for strings?
		//any other way than to cast here?
			case AVG:
				
				//handling if current tuple is the first tuple being merged
				if(resultAggregateTuples.size()==0) {
					
					Tuple aggTup = new Tuple(memTupleDesc);
					aggTup.setField(0, t.getField(0));
					resultAggregateTuples.add(aggTup);
				}
				//first tuple has been handled
				else {
					//times the old average by the number of tuples to get the actual value
					int tupleVal = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString()) * count;
					//calculate new average  by adding new tuple's value and divide by the new num of tuples
					int newAverage = tupleVal + Integer.parseInt(t.getField(0).toString()) / (count + 1);
					
					Tuple newTupleAVG = new Tuple(memTupleDesc);
					newTupleAVG.setField(0, new IntField(newAverage));
					
					resultAggregateTuples.set(0,newTupleAVG);
					
				}

				

				
				
			case COUNT:
				//handling if current tuple is the first tuple being merged
				if(resultAggregateTuples.size()==0) {
					
					Tuple aggTup = new Tuple(memTupleDesc);
					aggTup.setField(0, t.getField(0));
					resultAggregateTuples.add(aggTup);
				}
				//first tuple has been handled
				else {
					//every time tuple is merged just add one to pre-existing count
					int newCount = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString()) + 1;
					Tuple newTupleCount = new Tuple(memTupleDesc);
					newTupleCount.setField(0, new IntField(newCount));
					
					resultAggregateTuples.set(0,newTupleCount);					
				}
				

				
				
			case SUM:
				//handling if current tuple is the first tuple being merged
				if(resultAggregateTuples.size()==0) {
					
					Tuple aggTup = new Tuple(memTupleDesc);
					IntField newField = (IntField) t.getField(0);
					aggTup.setField(0, newField);
					resultAggregateTuples.add(aggTup);
				}
				//first tuple has been handled
				else {		
					//every time tuple is merged just add curr tuples value to pre-existing sum
					int oldSum = Integer.parseInt(resultAggregateTuples.get(0).getField(0).toString());
					int newSum = oldSum + Integer.parseInt(t.getField(0).toString());
					
					Tuple newTupleSum = new Tuple(memTupleDesc);
					newTupleSum.setField(0, new IntField(newSum));
					
					resultAggregateTuples.set(0,newTupleSum);
				}
		
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
