package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 * 
 * Student: Ben Fletcher 498067
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		tuples = l;
		this.td = td;
		
		
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		
		ArrayList<Tuple> selectArrayList = new ArrayList<Tuple>();
		
		//all tuples that match the select condition
		for(Tuple t: tuples) {
			
			if(t.getField(field).compare(op, operand)== true) {
				selectArrayList.add(t);
			}
				
		}
			
		Relation newRelation = new Relation(selectArrayList,td);
		
		return newRelation;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		Type[] newTypes = new Type[td.numFields()];
		String[] newNames = new String[td.numFields()];
		
		for(int i = 0; i<td.numFields();i++) {
			
			if(fields.contains(i)) {
				newTypes[i] = td.getType(i);
				newNames[i] = names.get(i);
			}
			else {
				newTypes[i] = td.getType(i);
				newNames[i] = td.getFieldName(i);
			}
			
		}
		
		TupleDesc newTupleDesc = new TupleDesc(newTypes, newNames);
		
		Relation newRelation = new Relation(tuples, newTupleDesc);
		
		return newRelation;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		
		Type[] projectTypes = new Type[fields.size()];
		String[] projectNames = new String[fields.size()];
		
		
		//create new tuple desc for this project relation
		for(int i = 0; i<fields.size();i++) {
			projectTypes[i] = td.getType(fields.get(i));
			projectNames[i] = td.getFieldName(fields.get(i));
		}

		TupleDesc projectTupleDesc = new TupleDesc(projectTypes, projectNames);
		
		//make sure all tuples in current relation are converted to only include necessary fields
		ArrayList<Tuple> projectTuples = tuples;
		
		for(Tuple t: projectTuples) {
			t.setDesc(projectTupleDesc);
		}
		
		Relation newRelation = new Relation(projectTuples,projectTupleDesc);
		
		return newRelation;
		
		
		
	}
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		
		Type[] combinedTypes = new Type[td.numFields()+ other.getDesc().numFields()];
		String[] combinedNames = new String[td.numFields()+ other.getDesc().numFields()];
		
		//first half of combined will be this.relation's types and names
		//second half will be filled with other.relation's types and names
		for(int i = 0;i<td.numFields();i++) {
			combinedTypes[i] = td.getType(i);
			combinedNames[i] = td.getFieldName(i);
		}
		for(int i = 0;i<other.getDesc().numFields();i++) {
			combinedTypes[i+td.numFields()] = other.getDesc().getType(i);
			combinedNames[i+td.numFields()] = other.getDesc().getFieldName(i);
		}
		
		TupleDesc joinDesc = new TupleDesc(combinedTypes,combinedNames);
		
	
		
		ArrayList<Tuple> combinedRelationTuples = new ArrayList<Tuple>(tuples.size()+ other.getTuples().size());
		
		for(int i = 0; i<tuples.size();i++) {
			
			for(int j = 0; j<other.getTuples().size();j++) {
				
				//if the condition is met, that both relations indicated fields are equal
				if(tuples.get(i).getField(field1).compare(RelationalOperator.EQ, other.getTuples().get(j).getField(field2)) == true) {
					Tuple combinedNewTuple = new Tuple(joinDesc);
					
					//set all fields to be equal to partitioned fields
					for(int x = 0; x<combinedNames.length;i++) {
						
						//we know first half is this.relation and second half is other.relation
						if(x< td.numFields()) {
							combinedNewTuple.setField(x, tuples.get(i).getField(x));
						}
						//other.relation
						else {
							combinedNewTuple.setField(x, other.getTuples().get(j).getField(x));
						}
						
						//add filled tuples to new tuple array list containing joined tuples
						combinedRelationTuples.add(combinedNewTuple);
					}
				}
			}
			
		}
		
		Relation joinedRelation = new Relation(combinedRelationTuples,joinDesc);
		
		return joinedRelation;
	}
	
	/**
	 * for use in piecing back together projects in queries
	 * @param other
	 * @return relation that is two relations joined together on no condition
	 */
	public Relation joinProjectionsTogether(Relation other) {
		
		Type[] combinedTypes = new Type[td.numFields()+ other.getDesc().numFields()];
		String[] combinedNames = new String[td.numFields()+ other.getDesc().numFields()];
		
		//first half of combined will be this.relation's types and names
		//second half will be filled with other.relation's types and names
		for(int i = 0;i<td.numFields();i++) {
			combinedTypes[i] = td.getType(i);
			combinedNames[i] = td.getFieldName(i);
		}
		for(int i = 0;i<other.getDesc().numFields();i++) {
			combinedTypes[i+td.numFields()] = other.getDesc().getType(i);
			combinedNames[i+td.numFields()] = other.getDesc().getFieldName(i);
		}
		
		TupleDesc joinDesc = new TupleDesc(combinedTypes,combinedNames);
		
		ArrayList<Tuple> combinedRelationTuples = new ArrayList<Tuple>(tuples.size()+ other.getTuples().size());
		
		for(int i = 0; i<tuples.size();i++) {
			
			for(int j = 0; j<other.getTuples().size();j++) {
				
					Tuple combinedNewTuple = new Tuple(joinDesc);
					
					//set all fields to be equal to partitioned fields
					for(int x = 0; x<combinedNames.length;i++) {
						
						//we know first half is this.relation and second half is other.relation
						if(x< td.numFields()) {
							combinedNewTuple.setField(x, tuples.get(i).getField(x));
						}
						//other.relation
						else {
							combinedNewTuple.setField(x, other.getTuples().get(j).getField(x));
						}
						
						//add filled tuples to new tuple array list containing joined tuples
						combinedRelationTuples.add(combinedNewTuple);
					}
				}
			}
			
		
		Relation joinedRelation = new Relation(combinedRelationTuples,joinDesc);
		
		return joinedRelation;

		
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		
		Aggregator agg = new Aggregator(op,groupBy,this.td);
		
		//merge each indv tuple in current relation
		for(Tuple t:this.tuples) {
			agg.merge(t);
		}
		
		//get result tuple arraylist
		ArrayList<Tuple> returnedAggAL = agg.getResults();
		
		//make new relation with same td
		Relation aggRelation = new Relation(returnedAggAL, this.td);
		
		return aggRelation;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		
		//add tuple desc
		String concat = td.toString() + " ";
		
		//add all tuples to end
		for(Tuple t: tuples) {
			concat += t.toString() + " ";
		}
		
		return concat;
	}
}
