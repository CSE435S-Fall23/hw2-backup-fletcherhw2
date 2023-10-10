package hw1;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
/*
 * Student 1 name: Ben Fletcher
 * Date: 9/6/2023
 */
public class Tuple {
	
	private TupleDesc tupleSchema;
	private HashMap<Integer, Field> tupleStorage;
	private int tuplePID;
	private int tupleSlotID;
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	
	public Tuple(TupleDesc t) {
		//your code here
		tupleSchema = t;
		//initialize hashmap with correct length
		tupleStorage = new HashMap<Integer,Field>(t.numFields());
		
	}
	
	public TupleDesc getDesc() {
		//your code here
		return tupleSchema;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return tuplePID;
	}

	public void setPid(int pid) {
		//your code here
		tuplePID = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return tupleSlotID;
	}

	public void setId(int id) {
		//your code here
		tupleSlotID = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		tupleSchema = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		tupleStorage.put(i, v);
	}
	
	public Field getField(int i) {
		//your code here
		return tupleStorage.get(i);
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		String concatString = "";
		
		for(int i = 0; i<tupleStorage.size();i++) {
			concatString += tupleSchema.getFieldName(i) + ": " + tupleStorage.get(i) + ", ";
		}
		return concatString;
	}
}
	