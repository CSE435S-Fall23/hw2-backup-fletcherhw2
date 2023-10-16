/**
 * Student: Ben Fletcher 498067
 */
package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
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
		//each operation will work on this starter relation
		ArrayList<Tuple> allTuples = hf.getAllTuples();
		TupleDesc initDesc = hf.getTupleDesc();
		
		Relation starterRelation = new Relation(allTuples,initDesc);
		
		
	
		
		ArrayList<ColumnVisitor> visList = new ArrayList<ColumnVisitor>();
		ArrayList<WhereExpressionVisitor> whereVisList = new ArrayList<WhereExpressionVisitor>();
		
		List<SelectItem> listSelectColumns = sb.getSelectItems();
		List<Join> listJoins = sb.getJoins();
		ArrayList<String> projectedColumns = new ArrayList<String>();
		
		
		//handle joins first, not efficient but makes it easier
		for(Join j: listJoins) {
			
			Expression onE = j.getOnExpression();
			
			//from piazza student answer question @84
			BinaryExpression be = (BinaryExpression) onE;
			
			Column le = (Column)be.getLeftExpression();
			Column re = (Column)be.getRightExpression();
			
			
			int fieldL = initDesc.nameToId(le.getColumnName());
			int fieldR = initDesc.nameToId(re.getColumnName());
			
			String leftTable = heapfileName;
			String rightTable = j.getRightItem().toString();
			
			//get the heap file on the right side of the join
			int tableIDRight = Database.getCatalog().getTableId(rightTable);
			HeapFile hfRight = Database.getCatalog().getDbFile(tableIDRight);
					
			//get all tuples and the tuple description and make the starter relation
			//each operation will work on this starter relation
			ArrayList<Tuple> allTuplesRight = hfRight.getAllTuples();
			TupleDesc initDescRight = hfRight.getTupleDesc();
					
			Relation starterRelationRight = new Relation(allTuplesRight,initDescRight);
			
			starterRelation = starterRelation.join(starterRelationRight, fieldL, fieldR);
		}
		
		
		
		//loop through all columns listed after the select
		for(SelectItem si:listSelectColumns) {
			
			ColumnVisitor cv = new ColumnVisitor();
			WhereExpressionVisitor we = new WhereExpressionVisitor();
			//grab select columns
			si.accept(cv);
			//grab where clause details
			si.accept(we);
			
			
			visList.add(cv);
			whereVisList.add(we);
			
		}
		
		
		//select the rows we want
		for(WhereExpressionVisitor whereV:whereVisList) {
			
		}
		
		
		
		//project the final columns that are after the select clause
		
		for(ColumnVisitor columnv: visList) {
			
			//handle select all
			if(columnv.getColumn()=="*") {

			}
			//if column is aggregate
			if(columnv.isAggregate()) {
				AggregateOperator columnAGGREGATE = columnv.getOp();
				starterRelation.aggregate(columnAGGREGATE, false);
			}
			//column is just a normal, non * column
			else 
			{
				//projectedColumns
			}
			
		}
		
		
		
		
		
		
		return starterRelation;
		
	}
}
