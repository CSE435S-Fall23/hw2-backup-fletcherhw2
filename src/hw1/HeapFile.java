package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
/*
 * Student 1 name: Ben Fletcher
 * Date: 9/6/2023
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File memFile;
	private TupleDesc memSchema;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		memFile = f;
		memSchema = type;
	}
	
	public File getFile() {
		//your code here
		return memFile;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return memSchema;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws FileNotFoundException 
	 */
	public HeapPage readPage(int id){
		//your code here
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(memFile, "rw");
		} catch (FileNotFoundException e2) {
			System.out.println("error in readPage(int id) method; step1");
		}
		
		long offset = id * this.PAGE_SIZE;
		int readByteNum = 0;
		
		try {
			//find the heap page by moving the to the start of the bytes 
			//corresponding to that page
			raf.seek(offset);
		} catch (IOException e1) {
			System.out.println("error in readPage(int id) method; step2");
		}
		
		byte[] inputBytes = new byte[PAGE_SIZE];
		
		try {
			//read the page's 4096 bytes in total
			//TODO: check how many bytes are read here 
			readByteNum = raf.read(inputBytes, 0, PAGE_SIZE);
		} catch (IOException e1) {
			System.out.println("error in readPage(int id) method; step3");
		}
		
		HeapPage hp = null;
		
		try {
			//why are bytes besides 0 and 1 being entered into the header here?
			hp = new HeapPage(id,inputBytes,this.getId());
			
		} catch (IOException e) {
			
			System.out.println("failed to instantiate heapfile in HeapFile.readPage(int id); step4");
		}
		
		try {
			raf.close();
		} catch (IOException e) {
			System.out.println("error in readPage(int id) method; step5");
		}
		
		return hp;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		//method found from oracle docs
		int fileToHash = memFile.hashCode();
		return fileToHash;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws FileNotFoundException 
	 */
	public void writePage(HeapPage p){
		//your code here
		int hpID = p.getId();
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(memFile, "rw");
		} catch (FileNotFoundException e1) {
			System.out.println("error in writing heap file to disk in writePage(HeapPage p) method; step1");
		}

		try {
			byte[] bytes = p.getPageData();
			raf.seek(hpID*PAGE_SIZE);
			long point = raf.getFilePointer();
			raf.write(bytes);
			//raf.write(bytes, hpID*PAGE_SIZE, PAGE_SIZE);
		} catch (IOException e) {
			System.out.println("error in writing heap file to disk in writePage(HeapPage p) method; step2");
		}
		
		try {
			raf.close();
		} catch (IOException e) {
			System.out.println("error in writing heap file to disk in writePage(HeapPage p) method; step3");
		}
		
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws FileNotFoundException 
	 */
	public HeapPage addTuple(Tuple t) throws FileNotFoundException {
		//your code here
		
		//search for empty heap page
		//if empty add to this page

		int i = 0;
		int numPages = getNumPages();
		HeapPage hp = null;
		boolean doneRunning = false;
		
		
		while(i<numPages || doneRunning == false) {
			
			hp = this.readPage(i);

			
			if(hp.emptySlotExists()) {
				doneRunning = true;
				try {
					hp.addTuple(t);
				} catch (Exception e) {
					System.out.println("error in addTuple() method; step1" + e.getMessage());
				}
				
				this.writePage(hp);
				return hp;
			}
			
			i++;

		}
		
		//no empty pages so make a new page to store tuple
		HeapPage hpNew = null;
		byte[] input = new byte[PAGE_SIZE];
		
		
		try {
			
			hpNew = new HeapPage(numPages, input, this.getId());
			
			try {
				hpNew.addTuple(t);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			System.out.println("error in addTuple() method; step2");
		}
		
		this.writePage(hpNew);
		
		
		return hpNew;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws FileNotFoundException 
	 */
	public void deleteTuple(Tuple t) throws FileNotFoundException{
		//your code here
		int tuplePageID = t.getPid();
		
		//read page from disk
		HeapPage hp = this.readPage(tuplePageID);
		
		//call the page's delete method
		try {
			hp.deleteTuple(t);
			
		} catch (Exception e) {
			
			System.out.println("Page id doesnt map to a page, in deleteTuple(Tuple t) method in HeapFile class");
		}
		
		this.writePage(hp);
		
		
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 * @throws IOException 
	 */
	public ArrayList<Tuple> getAllTuples(){
		//your code here
		ArrayList<Tuple> combinedTuples = new ArrayList<Tuple>();
		HeapPage currHeap;
		
		int numPages = this.getNumPages();
		int i = 0;
		
		while(i<numPages) {
			
			currHeap = this.readPage(i);

			for(Tuple tuple: currHeap.getTupleAr()) {
				
				//needed because null items in the Arraylist are counted in size()
				//probably a different way to fix this but this works
				if(tuple != null) {
					combinedTuples.add(tuple);
				}
				
			}
			
			i++;
		}
		
		
		//addAll method with array from heap page should work here
		return combinedTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(memFile, "rw");
		} catch (FileNotFoundException e1) {
			System.out.println("Error in getNumPages; step1");
		}
		
		byte[] data = new byte[PAGE_SIZE];
		boolean looping = true;
		int counter = 0;
		
		while(looping) {
					
			try {
				//if end of file is reached
				if(raf.read(data, 0, PAGE_SIZE) <0) {
					looping = false;
				}
			} catch (IOException e) {
				System.out.println("Error in getNumPages; step3");
			}
			
			if(looping == true) {
				counter++;
				data = new byte[PAGE_SIZE];
			}
		}
		
		try {
			raf.close();
		} catch (IOException e) {
			System.out.println("Error in getNumPages; step4");
		}
		return counter;
	}
}
