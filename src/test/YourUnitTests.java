package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.Query;
import hw1.Relation;


public class YourUnitTests{
	
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
	}
	
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
	
}