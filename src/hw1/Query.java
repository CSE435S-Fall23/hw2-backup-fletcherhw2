/**
 * Student: Ben Fletcher 498067
 */
package hw1;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
		
		boolean groupByFlag = false;
		
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
		
		List<SelectItem> listSelectColumns = sb.getSelectItems();
		List<Join> listJoins = sb.getJoins();
		
		
	
		//handle joins first, not efficient but makes it easier
		if(listJoins != null) {
			for(Join j: listJoins) {
				
				Expression onE = j.getOnExpression();
				
				//from piazza student answer question @84
				BinaryExpression be = (BinaryExpression) onE;
				
				Column le = (Column)be.getLeftExpression();
				Column re = (Column)be.getRightExpression();
				
				String colL = le.getColumnName();
				String colR = re.getColumnName();
				
				
				
				String leftTable = heapfileName;
				String rightTable = j.getRightItem().toString();
				
				//get the heap file on the right side of the join
				int tableIDRight = Database.getCatalog().getTableId(rightTable);
				HeapFile hfRight = Database.getCatalog().getDbFile(tableIDRight);
				
				

						
				//get all tuples and the tuple description and make the starter relation
				//each operation will work on this starter relation
				ArrayList<Tuple> allTuplesRight = hfRight.getAllTuples();
				TupleDesc initDescRight = hfRight.getTupleDesc();
				
				int fieldL = initDesc.nameToId(colL);
				int fieldR = hfRight.getTupleDesc().nameToId(colR);
						
				Relation starterRelationRight = new Relation(allTuplesRight,initDescRight);
				
				starterRelation = starterRelation.join(starterRelationRight, fieldL, fieldR);
			}			
		}

	
		
		//get where statements
		if(sb.getWhere()!= null) {
			WhereExpressionVisitor we = new WhereExpressionVisitor();
			Expression whereE = sb.getWhere();
			whereE.accept(we);	
			
			String lwhere = we.getLeft();
			Field rwhere = we.getRight();
			RelationalOperator opR = we.getOp();
			
			//call select so only the correct rows are in the relation
			starterRelation = starterRelation.select(initDesc.nameToId(lwhere), opR, rwhere);
		}

		//get groupBy
		if(sb.getGroupByColumnReferences()!= null) {
			groupByFlag = true;
			
		}
		
		
		//needed because visitor overwrites renames
		//will occur at end of code after other operations have finished
		ArrayList<Integer> renamedFields = new ArrayList<Integer>();
		ArrayList<String> renamedNames = new ArrayList<String>();
		//will be in the form <oldname, newname>
		Dictionary<String,String> renamedNamesDict = new Hashtable<String,String>();
		
		
		//loop through all columns listed after the select
		for(SelectItem si:listSelectColumns) {
			
			ColumnVisitor cv = new ColumnVisitor();
			
			//grab select columns
			si.accept(cv);
	
			visList.add(cv);
			
			//make sure column can be renamed later but doesnt lose its value
			//select all causes error here
			if(cv.getColumn() != "*") {
				
				SelectExpressionItem sic = (SelectExpressionItem) si;
				//add oldname and new name to dictionary
				if(sic.getAlias()!=null) {
					
					renamedNamesDict.put(cv.getColumn(),sic.getAlias().getName());
					renamedNames.add(sic.getAlias().getName());
				}				
			}

			
		}
		
		
		//project the final columns that are after the select clause
		
		Relation projectedRelation = starterRelation;
		ArrayList<Integer> projectedFields = new ArrayList<Integer>();

		for(ColumnVisitor columnv: visList) {
			
			//handle select all, return all columns
			if(columnv.getColumn() == "*") {
				return starterRelation;
			}
			//if column is aggregate
			if(columnv.isAggregate()) {
				
				if(groupByFlag) {
					AggregateOperator columnAGGREGATE = columnv.getOp();
					projectedRelation = starterRelation.aggregate(columnAGGREGATE, true);					
				}
				else {
					AggregateOperator columnAGGREGATE = columnv.getOp();
					
					//project only the singular column before it is aggregated
					ArrayList<Integer> temp = new ArrayList<Integer>();
					temp.add(initDesc.nameToId(columnv.getColumn()));
					
					Relation tempRelation = starterRelation.project(temp);
					
					projectedRelation = tempRelation.aggregate(columnAGGREGATE, false);
				}
				
				return projectedRelation;

			}
			//column is just a normal, non * column
			else 
			{
				projectedFields.add(starterRelation.getDesc().nameToId(columnv.getColumn()));
			}
			
		}
		
		
		//project all of the necessary fields gained from the visitors
		projectedRelation = projectedRelation.project(projectedFields);
		
		
		
		//call rename relation on final projection
		if(renamedNames.size()!=0) {
			
			//rename after everything important has been done
			//AS clause
			Enumeration<String> oldFields = renamedNamesDict.keys();
			
			//get the old field ids from the old names
			while(oldFields.hasMoreElements()) {
				renamedFields.add(projectedRelation.getDesc().nameToId(oldFields.nextElement()));
			}
			
			projectedRelation = projectedRelation.rename(renamedFields, renamedNames);	
		}
	

		
		
		//starterRelation
		return projectedRelation;
		
	}
}
