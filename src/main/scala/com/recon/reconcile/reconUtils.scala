package com.recon.reconcile

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}

class reconUtils {
  
  def fetchDataViewNameColumns(ruleDataRecord :ruleDataViewRecord ): HashMap[String, HashSet[ArrayBuffer[String]]] = {
      var ruleDataRec = new HashMap[String, HashSet[ArrayBuffer[String]]]()
      var sourceColNames = new HashSet[ArrayBuffer[String]]()
      var targetColNames = new HashSet[ArrayBuffer[String]]()
      
      for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions ) {
        if (ruleCond.sRefDvColumn != null) {
            var srcColumnsDtlList = new ArrayBuffer[String]()
            srcColumnsDtlList.append(ruleCond.sRefDvColumn)
            srcColumnsDtlList.append(ruleCond.sColumnName)
            srcColumnsDtlList.append(ruleCond.sColDataType)
            srcColumnsDtlList.append(ruleCond.sColDataFormat)
            sourceColNames.add(srcColumnsDtlList)
        }
        
        if (ruleCond.tRefDvColumn != null) {
            var tarColumnsDtlList = new ArrayBuffer[String]()
            tarColumnsDtlList.append(ruleCond.tRefDvColumn)
            tarColumnsDtlList.append(ruleCond.tColumnName)
            tarColumnsDtlList.append(ruleCond.tColDataType)
            tarColumnsDtlList.append(ruleCond.tColDataFormat)
            targetColNames.add(tarColumnsDtlList)
        }
        
        ruleDataRec.put(ruleDataRecord.sourceViewName, sourceColNames)
        ruleDataRec.put(ruleDataRecord.targetViewName, targetColNames)
      }
      
      ruleDataRec
  }
  
  def getSelSqlWithAliasAndCast(viewColumn : HashSet[ArrayBuffer[String]]) : String = {
    
      var selectSQL : String = " scrIds, "
      var colIterator: Iterator[ArrayBuffer[String]] = viewColumn.toIterator

      while (colIterator.hasNext) {
        
        var colsIter : List[String] = colIterator.next().toList
        
        if (colsIter(2) == null || colsIter(2).equals("") || 
            colsIter(2).equalsIgnoreCase( "varchar") )         {
           selectSQL += " lower(" + colsIter(1) + ") as " + colsIter(1) + ", "
        } else {
           if (colsIter(2).equalsIgnoreCase( "NUMBER") || colsIter(2).equalsIgnoreCase( "INTEGER") )  {
               selectSQL += "cast(" + colsIter(1) + " as int) as " + colsIter(1) + ", "
           } else if (colsIter(2).equalsIgnoreCase( "FLOAT"))  {
               selectSQL += "cast(" + colsIter(1) + " as float) as " + colsIter(1) + ", "
           } else if (colsIter(2).equalsIgnoreCase( "DECIMAL"))  {
               selectSQL += "cast(" + colsIter(1) + " as decimal(38,5)) as " + colsIter(1) + ", "
           } else if (colsIter(2).equalsIgnoreCase( "DATE"))  {
               selectSQL += "TO_DATE(CAST(UNIX_TIMESTAMP( "     +
                             colsIter(1) + ", '" + "yyyy-MM-dd" +
							               "') AS TIMESTAMP)) as "            +
							               colsIter(1).toString() + ", "
           }
        }
        
      }
      selectSQL.substring(0, selectSQL.length()-2 )
  }
}