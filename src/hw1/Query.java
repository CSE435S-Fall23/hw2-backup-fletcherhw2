/**
 * Student: Ben Fletcher 498067
 */
package hw1;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
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
		ArrayList<WhereExpressionVisitor> whereVisList = new ArrayList<WhereExpressionVisitor>();
		
		List<SelectItem> listSelectColumns = sb.getSelectItems();
		List<Join> listJoins = sb.getJoins();
		ArrayList<String> projectedColumns = new ArrayList<String>();
		
	/*	
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
	*/
		
		//get where statements
		if(sb.getWhere()!= null) {
			WhereExpressionVisitor we = new WhereExpressionVisitor();
			Expression whereE = sb.getWhere();
			whereE.accept(we);	
			
			String lwhere = we.getLeft();
			Field rwhere = we.getRight();
			RelationalOperator opR = we.getOp();
			
			//call select so only the correct rows are in the relation
			starterRelation.select(initDesc.nameToId(lwhere), opR, rwhere);
		}

		//get groupBy
		if(sb.getGroupByColumnReferences()!= null) {
			groupByFlag = true;
			
		}
		
		
		//needed because visitor overwrites renames
		//will occur at end of code after other operations have finished
		ArrayList<Integer> renamedFields = new ArrayList<Integer>();
		ArrayList<String> renamedNames = new ArrayList<String>();
		
		//loop through all columns listed after the select
		for(SelectItem si:listSelectColumns) {
			
			ColumnVisitor cv = new ColumnVisitor();
			
			//grab select columns
			si.accept(cv);
	
			visList.add(cv);
			
			//make sure column can be renamed later but doesnt lose its value
			SelectExpressionItem sic = (SelectExpressionItem) si;
			
			if(sic.getAlias()!=null) {
				renamedNames.add(sic.getAlias().getName());
			}
			
		}
		
		
		//project the final columns that are after the select clause
		
		Relation projectedRelation = starterRelation;
		/*
		ArrayList<Tuple> Al = new ArrayList<Tuple>();
		Type[] types = new Type[0];
		String[] names = new String[0];
		TupleDesc tdnew = new TupleDesc(types,names);
		Relation finalRelation = new Relation(Al,tdnew);
		*/
		for(ColumnVisitor columnv: visList) {
			
			//handle select all, return all columns
			if(columnv.getColumn() == "*") {
				return starterRelation;
			}
			//if column is aggregate
			if(columnv.isAggregate()) {
				
				if(groupByFlag) {
					AggregateOperator columnAGGREGATE = columnv.getOp();
					starterRelation.aggregate(columnAGGREGATE, true);					
				}
				else {
					AggregateOperator columnAGGREGATE = columnv.getOp();
					starterRelation.aggregate(columnAGGREGATE, false);
				}
				
				//finalRelation.joinProjectionsTogether(starterRelation);

			}
			//column is just a normal, non * column
			else 
			{
				/*
				ArrayList<Integer> fieldsAL = new ArrayList<Integer>();
				fieldsAL.add(hf.getTupleDesc().nameToId(columnv.getColumn()));
				
				projectedRelation = starterRelation.project(fieldsAL);
				
				
				finalRelation.joinProjectionsTogether(projectedRelation);
				
				*/
			}
			
		}
		
		
		//rename after everything important has been done
		for(String name: renamedNames) {
			renamedFields.add(hf.getTupleDesc().nameToId(name));
		}
		
		starterRelation.rename(renamedFields, renamedNames);
		
		
		
		
		
		
		return starterRelation;
		
	}
}
