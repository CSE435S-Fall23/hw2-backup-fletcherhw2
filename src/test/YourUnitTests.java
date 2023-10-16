package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;
import hw1.TupleDesc;


public class YourUnitTests{
	
	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;
	
	@Before
	public void setup() {
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		int tableId = c.getTableId("test");
		
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	//testing as query because tests given to use do not test this
	@Test
	public void QueryASTEST() {
		Query q = new Query("SELECT a2 AS secondColumn FROM A");
		Relation r = q.execute();
		
		assertTrue(r.getDesc().getFieldName(0).equals("secondColumn"));
		
		
		//same tests from testProject in other unit tests
		//to double check correctness
		assertTrue(r.getDesc().getSize() == 4);
		assertTrue(r.getTuples().size() == 8);

	}
	
	//test extra aggregator because tests given to use only test one(SUM)
	//this tests without groupby
	@Test
	public void extraAGGREGATETESTNOGB() {

		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.COUNT, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 36);


	}
	
	//test extra aggregator because tests given to use only test one(SUM)
	//this tests with groupby
	public void extraAGGREGATETESTYESGB() {
		
		
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.COUNT, true);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 36);


	}
	
}