package hw1;

import java.awt.List;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
/*
 * Student 1 name: Ben Fletcher
 * Date: 9/6/2023
 */
public class HeapPage {

	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;



	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		this.header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		return this.id;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		//your code here
		int nSlots = ((8 * Database.getCatalog().getDbFile(this.tableId).PAGE_SIZE )/(8 * td.getSize() + 1));
		return nSlots;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {        
		//your code here
		double test = Math.ceil(numSlots/8.0);
		int test1 = (int)test;
		return (int) Math.ceil(numSlots/8.0);
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		
		//your code here
		int bytePos = (int) Math.floor(s/8.0);
		int bitPos = Math.floorMod(s, 8);
		int bit = (header[bytePos]&(1<<bitPos));
		
		/*
		boolean isSet(byte value, int bit){
			   return (value&(1<<bit))!=0;
			} 
		*/
		/*
		if(s<header.length && header[s] != notfilled && header[s]!= filled) {
			header[s] = 0;
		}
		*/
		
		if(bit !=0 ) {
			return true;
		}
		
		return false;
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		
		int bytePos = (int) Math.floor(s/8.0);
		int bitPos = Math.floorMod(s, 8);
		int bit = (header[bytePos]&(1<<bitPos));
		
		byte newByte = (byte) ((header[bytePos])|(1<<bit));
		if(s>=0 && s<header.length) header[s] = newByte;
	}
	
	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		//your code here
		boolean justFilled = false;
		
		//check its tupleDesc first
		if(t.getDesc()!= td) {
			throw(new Exception("td invalid"));
		}
		
		boolean filledHeapPage = true;
		
		//check if heap page is filled
		for(int i = 0; i<header.length;i++) {
			
			if(header[i] == 0 && justFilled == false) {
				filledHeapPage = false;
				justFilled = true;
				tuples[i] = t;
				setSlotOccupied(i, true);
			}
			
		}
		
		if(filledHeapPage) {
			throw(new Exception("filled heap"));
		}
		
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		//your code here
		
		//check page id against tuples page id
		if(id != t.getPid()) {
			throw(new Exception("bad page id"));
		}
		
		//check if slot is already empty
		if(header[t.getId()] == 0) {
			throw(new Exception("slot already empty"));
		}
		
		//slot not empty so del tuple
		//and set bit to 0
		header[t.getId()] = 0;
		tuples[t.getId()] = null;
		
		
	}
	
	/**
     * Suck up tuples from the source file.
     */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
	 *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
		
		//clean up null tuples(not sure how else to do this)
		java.util.ArrayList<Tuple> aLT = new ArrayList<Tuple>();
		
		System.out.print(aLT.size());
		
		for(Tuple tup: this.tuples) {
			if(tup != null) {
				aLT.add(tup);
			}
		}
		System.out.print(aLT.size());
		
		
		return aLT.iterator();
	}
	/**
	 * Returns the member variable array of tuples, which will hopefully make returning an arraylist of all tuples
	 * easier
	 * @return
	 */
	public java.util.List<Tuple> getTupleAr() {
		return Arrays.asList(tuples);
	}
	
	/**
	 * returns if empty slot or not in this heap page
	 * @return
	 */
	public boolean emptySlotExists() {
		
		for(int i = 0; i< header.length;i++) {
			
			if(!(this.slotOccupied(i))) {
				return true;
			}
			
		}
		
		return false;
	}
}
