package Sharks;

public class SQLTerm {
	
	 String _strTableName;
	 String _strColumnName;
	 String _strOperator;
	 Object _objValue;
	 
	 
	 public SQLTerm ( String TableName, String ColumnName, 
			 String Operator, Object Value) {
		 
		 this._strTableName=TableName;
		 this._strColumnName= ColumnName;
		 this._strOperator= Operator;
		 this._objValue= Value;
		 
		 
		 
	 }

}
