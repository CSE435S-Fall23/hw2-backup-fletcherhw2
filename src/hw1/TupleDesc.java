package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
/*
 * Student 1 name: Ben Fletcher
 * Date: 9/6/2023
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	//in bytes
	private int sizeInt = 4;
	private int sizeString = 129;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	
    	types = typeAr;
    	fields = fieldAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	if(i < 0 || i > fields.length) {
    		throw(new NoSuchElementException());
    	}
    	
    	//is this needed?
    	if(fields[i] == null) {
    		return "";
    	}
    	
    	
    	return fields[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    	int index = -1;
    	
    	for(int i = 0; i<fields.length;i++) {
    		if(getFieldName(i).equals(name)) {
    			return i;
    		}
    	}
    	
    	if(index == -1) {
    		throw(new NoSuchElementException());
    	}
    	return 0;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	if(i<0 || i>types.length) {
    		throw(new NoSuchElementException());
    	}
    	
    	return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int sizeTuple = 0;
    	
    	for(int i = 0; i<fields.length;i++) {
    		
    		if(this.getType(i) == Type.INT) {
    			sizeTuple += sizeInt;
    		}
    		if(this.getType(i) == Type.STRING) {
    			sizeTuple += sizeString;
    		}
    	}
    	return sizeTuple;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	TupleDesc givenTuple = (TupleDesc) o;
    	
    	//compare size first
    	if(this.getSize() != givenTuple.getSize()) {
    		return false;
    	}
	
    	//compare elements between each object(if this is reached we know they have the same size)
    	for(int i = 0; i<fields.length;i++) {
    		
    		if(this.getType(i) != givenTuple.getType(i)) {
    			return false;
    		}
    	}
    	
    	//if elements are equal, size was already proven equal, so objects are equal
    	return true;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	String concatString = "";
    	
    	for(int i = 0; i<fields.length;i++) {
    		concatString += this.getType(i).name() + "(" + this.getFieldName(i) + "),";
    	}
    	
    	
    	return concatString;
    }
}
