/**
 * Student: Ben Fletcher 498067
 */
package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		//sb.getSelectItems();
		
		
		
		
		//get the heap file that is listed in the FROM clause
		String heapfileName = sb.getFromItem().toString();
		int tableID = Database.getCatalog().getTableId(heapfileName);
		HeapFile hf = Database.getCatalog().getDbFile(tableID);
		
		//get all tuples and the tuple description and make the starter relation
		ArrayList<Tuple> allTuples = hf.getAllTuples();
		TupleDesc initDesc = hf.getTupleDesc();
		
		Relation starterRelation = new Relation(allTuples,initDesc);
		
		
		
		ArrayList<ColumnVisitor> visList = new ArrayList<ColumnVisitor>();
		List<SelectItem> listSelectColumns = sb.getSelectItems();
		
		for(SelectItem si:listSelectColumns) {
			
			ColumnVisitor cv = new ColumnVisitor();
			si.accept(cv);
			visList.add(cv);
			System.out.println(cv.getColumn());
		}
		
		
		
		
		
		
		
		//your code here
		
		
		Relation executedRelation = new Relation(null,null);
		return executedRelation;
		
	}
}
