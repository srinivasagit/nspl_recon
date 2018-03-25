package com.recon.reconcile

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}
import scala.collection.Seq
import org.apache.spark.sql.{Dataset,Row,Column}
import org.apache.spark.sql.SparkSession

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
  
  def getSelSqlWithAliasAndCast(viewColumn : HashSet[ArrayBuffer[String]]) : ArrayBuffer[Column] = {
    
      var selectSQL : String = "scrIds" 
      var temp_t : ArrayBuffer[String] = ArrayBuffer.empty
      
      temp_t.append(selectSQL)
      
      var colIterator: Iterator[ArrayBuffer[String]] = viewColumn.toIterator

      while (colIterator.hasNext) {
        
        var colsIter : List[String] = colIterator.next().toList
        
        if ((colsIter(2) == null || colsIter(2).equals("") || 
             colsIter(2).equalsIgnoreCase( "varchar")) && (colsIter(1).contains(" ")))         {
            selectSQL = "lower(`" + colsIter(1) + "`) as `" + colsIter(1) + "`"  
            temp_t.append(selectSQL)
        } else if ((colsIter(2) == null || colsIter(2).equals("") || 
                    colsIter(2).equalsIgnoreCase( "varchar"))) {
            selectSQL = "lower(" + colsIter(1) + ") as " + colsIter(1)
            temp_t.append(selectSQL)          
        }
        else {
           if (colsIter(2).equalsIgnoreCase( "NUMBER") || colsIter(2).equalsIgnoreCase( "INTEGER") )  {
               selectSQL = "cast(" + colsIter(1) + " as int) as " + colsIter(1)
               temp_t.append(selectSQL)               
           } else if (colsIter(2).equalsIgnoreCase( "FLOAT"))  {
               selectSQL = "cast(" + colsIter(1) + " as float) as " + colsIter(1)
               temp_t.append(selectSQL)               
           } else if (colsIter(2).equalsIgnoreCase( "DECIMAL"))  {
               selectSQL = "cast(" + colsIter(1) + " as decimal(38,5)) as " + colsIter(1)
               temp_t.append(selectSQL)               
           } else if (colsIter(2).equalsIgnoreCase( "DATE"))  {
               selectSQL = "TO_DATE(CAST(UNIX_TIMESTAMP( "     +
                             colsIter(1) + ", '" + "yyyy-MM-dd" +
							               "') AS TIMESTAMP)) as "            +
							               colsIter(1).toString()
               temp_t.append(selectSQL)							               
           }
        }
        
      }
//      selectSQL.substring(0, selectSQL.length()-2 )
        temp_t.map(c => new Column(c))
  }
  
  def filterSourceData(spark : SparkSession, sData: Dataset[Row], ruleDataRecord :ruleDataViewRecord) : Dataset[Row] = {
    
    var sourceDataForReconFiltered : Dataset[Row] = sData
    import spark.sqlContext.implicits._
    import spark.implicits._
    
    for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {
    
      if (ruleCond.operator == null || ruleCond.operator.equals("") ) {
         if (ruleCond.sOperator != null) { 
            if (ruleCond.sOperator.equalsIgnoreCase("=") || ruleCond.sOperator.equalsIgnoreCase("EQUALS") ) {
              sourceDataForReconFiltered = sourceDataForReconFiltered.filter(ruleCond.sColumnName + " = \"" + ruleCond.sValue.toLowerCase() + "\"")
            } else if (ruleCond.sOperator.equalsIgnoreCase("CONTAINS")) {
              
//              val filterString = ruleCond.sColumnName + "."+ "contains( ruleCond.sValue.toLowerCase())" 
              sourceDataForReconFiltered = sourceDataForReconFiltered.filter(org.apache.spark.sql.functions.col(ruleCond.sColumnName).contains(ruleCond.sValue.toLowerCase()))
            }
         }
      }
      
    }
    
    if (sourceDataForReconFiltered != null) {
       sourceDataForReconFiltered
    } else {
       sData 
    }
    
  }

  def filterTargetData(spark : SparkSession, tData: Dataset[Row], ruleDataRecord :ruleDataViewRecord) : Dataset[Row] = {
    
    var targetDataForReconFiltered : Dataset[Row] = tData
    import spark.sqlContext.implicits._
    import spark.implicits._
    
    for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {
    
      if (ruleCond.operator == null || ruleCond.operator.equals("") ) {
         if (ruleCond.tOperator != null) { 
            if (ruleCond.tOperator.equalsIgnoreCase("=") || ruleCond.tOperator.equalsIgnoreCase("EQUALS") ) {
              targetDataForReconFiltered = targetDataForReconFiltered.filter( ruleCond.tColumnName + " = \"" + ruleCond.tValue.toLowerCase() + "\"")
            } else if (ruleCond.tOperator.equalsIgnoreCase("CONTAINS")) {
//              targetDataForReconFiltered = targetDataForReconFiltered.filter(ruleCond.tColumnName + "." + "contains('%" + ruleCond.tValue + "%')" )
              targetDataForReconFiltered = targetDataForReconFiltered.filter(org.apache.spark.sql.functions.col(ruleCond.tColumnName).contains(ruleCond.tValue.toLowerCase()))
            }
         }
      }
      
    }
    
    if (targetDataForReconFiltered != null) {
       targetDataForReconFiltered
    } else {
       tData 
    }
    
  }
  
  def getGroupByCols (ruleDataRecord :ruleDataViewRecord) : HashMap[String, ArrayBuffer[Column]] = {
  
    var srcTarGrpByMap : HashMap[String, ArrayBuffer[Column]] = new HashMap[String, ArrayBuffer[Column]]()
    
    var sGBy : String = ""
    var tGBy : String = ""
    
    for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {
      if (ruleCond.operator != null ) {
        if ((ruleCond.sColumnName != null) &&
            ( ! (sGBy.contains(ruleCond.sColumnName + "," ))) &&
            (ruleCond.sMany == null || ruleCond.sMany == false)) {
          
          sGBy+= ruleCond.sColumnName + ","
          
        }
        
        if ((ruleCond.tColumnName != null) &&
            (! (tGBy.contains(ruleCond.tColumnName + "," ))) &&
            (ruleCond.tMany == null || ruleCond.tMany == false)) {
          
          tGBy+= ruleCond.tColumnName + ","
          
        }        
        
      }
    }

    if (sGBy.length() > 1 ) {
      sGBy = sGBy.substring(0, sGBy.length()-1)
    }

    if (tGBy.length() > 1 ) {
      tGBy = tGBy.substring(0, tGBy.length()-1)
    }    
   
    var sColList : ArrayBuffer[Column] = new ArrayBuffer[Column]()
    var tColList : ArrayBuffer[Column] = new ArrayBuffer[Column]()
    
    for (cols :String <- sGBy.split(',')) {
      
      sColList+= new Column(cols)
    }

    for (cols :String <- tGBy.split(',')) {
      
      tColList+= new Column(cols)
    }    
    
    println ("Group By Column List : " + sColList  + " - " + tColList)
    
    srcTarGrpByMap.put("Source", sColList)
    srcTarGrpByMap.put("Target", tColList)
    srcTarGrpByMap
  }
  
   def getSmanyColumn (ruleDataRecord :ruleDataViewRecord): String = {
     
     var sManyCol: String = ""
     
     for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {
			 if (ruleCond.sMany != null && ruleCond.sMany ) {
				  sManyCol = ruleCond.sColumnName
			 }
     }
     sManyCol
   }
   
   def getTmanyColumn (ruleDataRecord :ruleDataViewRecord): String = {
    
     var tManyCol: String = ""
     
     for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {
			 if (ruleCond.tMany != null && ruleCond.tMany ) {
				  tManyCol = ruleCond.tColumnName
			 }
     }
     tManyCol
   }
  
   def filterSourceDataBasedOnRuleType(sourceDataForRecon: Dataset[Row], 
                                       keyCols : HashMap[String, ArrayBuffer[Column]], 
                                       ruleDataRecord :ruleDataViewRecord) : Dataset[Row] = {
    
     var sourceDataForReconFiltered : Dataset[Row] = sourceDataForRecon		
     
     if (ruleDataRecord.ruleType.startsWith("ONE")) {
       
        var uniqueSourceIds = sourceDataForRecon.groupBy(keyCols("Source") : _*).count.filter("count = 1")
       
        sourceDataForReconFiltered = sourceDataForRecon.join(uniqueSourceIds, keyCols("Source").map(r => r.toString).toSeq)
                                                       .drop(uniqueSourceIds.col("count"))
                                                       
     } else if (ruleDataRecord.ruleType.startsWith("MANY")) {
        
       var multiSourceIds = sourceDataForRecon.groupBy(keyCols("Source") : _*).count.filter("count > 1")

       sourceDataForReconFiltered = sourceDataForRecon.join(multiSourceIds, keyCols("Source").map(r => r.toString).toSeq)
                                                      .drop(multiSourceIds.col("count"))
        
       var SManyCol : String = getSmanyColumn(ruleDataRecord) 
       var old = "sum(" + SManyCol + ")"
       
       sourceDataForReconFiltered = sourceDataForReconFiltered.groupBy(keyCols("Source") : _*)
                                                              .sum(SManyCol)
                                                              .withColumnRenamed(old, "SUMTOTAL_TEMP")
     }
     sourceDataForReconFiltered
     
   }

   def filterTargetDataBasedOnRuleType(targetDataForRecon: Dataset[Row], 
                                       keyCols : HashMap[String, ArrayBuffer[Column]], 
                                       ruleDataRecord :ruleDataViewRecord) : Dataset[Row] = {
    
     var targetDataForReconFiltered : Dataset[Row] = targetDataForRecon		
     
     if (ruleDataRecord.ruleType.endsWith("ONE")) {
       
        var uniqueTargetIds = targetDataForRecon.groupBy(keyCols("Target") : _*).count.filter("count = 1")
       
        targetDataForReconFiltered = targetDataForRecon.join(uniqueTargetIds, keyCols("Target").map(r => r.toString).toSeq)
                                                       .drop(uniqueTargetIds.col("count"))
                                                       
     } else if (ruleDataRecord.ruleType.endsWith("MANY")) {
        
        var multiTargetIds = targetDataForRecon.groupBy(keyCols("Target") : _*).count.filter("count > 1")

        targetDataForReconFiltered = targetDataForRecon.join(multiTargetIds, keyCols("Target").map(r => r.toString).toSeq)
                                                       .drop(multiTargetIds.col("count"))
        
        var TManyCol : String = getTmanyColumn(ruleDataRecord) 
        var old = "sum(" + TManyCol + ")"
       
        targetDataForReconFiltered = targetDataForReconFiltered.groupBy(keyCols("Target") : _*)
                                                               .sum(TManyCol)
                                                               .withColumnRenamed(old, "SUMTOTAL_TEMP")
     }
     targetDataForReconFiltered
     
   }
   
   def getWhereClauseFor (ruleDataRecord :ruleDataViewRecord) : String = {
     var whereClause : String = " "
     
     for (ruleCond: ruleConditionRecord <- ruleDataRecord.ruleConditions) {

       if  (ruleCond.operator != null) {
					 var sourceCol: String = ""
					 var operator: String = ""
					 var targetCol : String = ""
					 var sourceTagetExpression: String = ""
					 var logicalOperator : String= ""
					 
           // check operator
					 
					 if (ruleCond.operator.trim().equals("=") ||
					     ruleCond.operator.trim().equalsIgnoreCase("EQUALS")) {
					     operator = "="
					 } else if (ruleCond.operator.trim().equals("<") ||
					     ruleCond.operator.trim().equalsIgnoreCase("LESS THAN")) {
					     operator = "<"
					 } else if (ruleCond.operator.trim().equals("<=") ||
					     ruleCond.operator.trim().equalsIgnoreCase("LESS THAN OR EQUAL")) {
					     operator = "<="
					 } else if (ruleCond.operator.trim().equals(">") ||
					     ruleCond.operator.trim().equalsIgnoreCase("GREATER THAN")) {
					     operator = ">"
					 } else if (ruleCond.operator.trim().equals(">=") ||
					     ruleCond.operator.trim().equalsIgnoreCase("GREATER THAN OR EQUAL") ||
					     ruleCond.operator.trim().equalsIgnoreCase("GREATER THAN EQUAL") ) {
					     operator = ">="
					 } else if (ruleCond.operator.trim().equals("!=") ||
					     ruleCond.operator.trim().equalsIgnoreCase("NOT EQUALS")) {
					     operator = "!="
					 } else if (ruleCond.operator.trim().equalsIgnoreCase("CONTAINS")) {
					     operator = " like concat('%',"
					 } else if (ruleCond.operator.trim().equalsIgnoreCase("BEGINS WITH")) {
					     operator = " like concat("
					 } else if (ruleCond.operator.trim().equalsIgnoreCase("ENDS WITH")) {
					     operator = " like concat('%'"
					 } 
					 
					 if (ruleCond.sColumnName != null) {
					    sourceCol = "src." + ruleCond.sColumnName
					    println("sourceCol : " + sourceCol)
					 }
					 if ((ruleCond.sMany != null) && (ruleCond.sMany == true )) {
//					 if ((ruleCond.sMany != null) && (ruleCond.sMany == '1' )) {
					    sourceCol = "src.SUMTOTAL_TEMP"
					    println("sourceCol at sMany: " + sourceCol)
					 }
					 if (ruleCond.tColumnName != null) {
					    targetCol = "tar." + ruleCond.tColumnName
					 }
					 if ((ruleCond.tMany != null) && (ruleCond.tMany == true )) {
//					 if ((ruleCond.tMany != null) && (ruleCond.tMany == '1' )) {
					    targetCol = "tar.SUMTOTAL_TEMP"
					 }

					 if (ruleCond.sFormula != null) {
					   if (ruleCond.sFormula.toLowerCase().contains("field")) {
					       sourceCol = ruleCond.sFormula.replaceAll("field", sourceCol)
					   }
					 }

					 if (ruleCond.tFormula != null) {
					   if (ruleCond.tFormula.toLowerCase().contains("field")) {
					       targetCol = ruleCond.tFormula.replaceAll("field", targetCol)
					   }
					 }
					 
					 if (ruleCond.operator.trim().equalsIgnoreCase("CONTAINS") ||
					     ruleCond.operator.trim().equalsIgnoreCase("BEGINS WITH")) {
					     targetCol = targetCol + ",'%')"
					 } else if (ruleCond.operator.trim().equalsIgnoreCase("ENDS WITH")) {
					     targetCol = targetCol + ")"
					 }

					 if (ruleCond.sToleranceType != null &&
					    (ruleCond.sToleranceType.equalsIgnoreCase("DAY")    ||
					     ruleCond.sToleranceType.equalsIgnoreCase("DAYS")) ) {
					     
					     var lowerBound : String = ""
					     var upperBound : String = ""
					     if (ruleCond.sToleranceOperatorFrom != null ) {
					        lowerBound = ruleCond.sToleranceOperatorFrom
					     }
					     lowerBound = lowerBound + ruleCond.sToleranceValueFrom
					     
					     if (ruleCond.sToleranceOperatorTo != null) {
					        upperBound = ruleCond.sToleranceOperatorTo
					     }
					     upperBound = upperBound + ruleCond.sToleranceValueTo
					     
					     sourceTagetExpression = " " + sourceCol +
					                             " BETWEEN DATE_ADD(" + targetCol + "," + lowerBound.toInt +") AND " +
					                             " DATE_ADD(" + targetCol + "," + upperBound.toInt + ") "
					 } else if (ruleCond.tToleranceType != null &&
					           (ruleCond.tToleranceType.equalsIgnoreCase("DAY")    ||
					            ruleCond.tToleranceType.equalsIgnoreCase("DAYS")) ) {
					     
					           var lowerBound : String = ""
					           var upperBound : String = ""
					           
					           if (ruleCond.tToleranceOperatorFrom != null ) {
					              lowerBound = ruleCond.tToleranceOperatorFrom
					           }
					           
					           lowerBound = lowerBound + ruleCond.tToleranceValueFrom
					     
					           if (ruleCond.tToleranceOperatorTo != null) {
					               upperBound = ruleCond.tToleranceOperatorTo
					           }
      					     
					           upperBound = upperBound + ruleCond.tToleranceValueTo
					     
	      				     sourceTagetExpression = " " + targetCol +
  				                                   " BETWEEN DATE_ADD(" + sourceCol + "," + lowerBound.toInt +") AND " +
					                                   " DATE_ADD(" + sourceCol + "," + upperBound.toInt + ") "
					 }

					 if (ruleCond.sToleranceType != null &&
					    (ruleCond.sToleranceType.equalsIgnoreCase("AMOUNT")) ) {
					     
					     var lowerBound : String = ""
					     var upperBound : String = ""
					     
					     if (ruleCond.sToleranceOperatorFrom != null ) {
					        lowerBound = ruleCond.sToleranceOperatorFrom
					     }
					     if (ruleCond.sToleranceValueFrom.contains("%")) {
					        var temp : String = ruleCond.sToleranceValueFrom.replaceAll("%", "")
					        lowerBound = lowerBound + " ((" + targetCol + " * " + temp + ")/100)" 
					        
					     } else {
					        lowerBound = lowerBound + ruleCond.sToleranceValueFrom
					     }
					     
					     if (ruleCond.sToleranceOperatorTo != null) {
					        upperBound = ruleCond.sToleranceOperatorTo
					     }
					     
					     if (ruleCond.sToleranceValueTo.contains("%")) {
					        var temp : String = ruleCond.sToleranceValueTo.replaceAll("%", "")
					        upperBound = upperBound + " ((" + targetCol + " * " + temp + ")/100)" 
					        
					     } else {
					        upperBound = upperBound + ruleCond.sToleranceValueTo
					     }					     
					     
					     sourceTagetExpression = " " + sourceCol +
					                             " BETWEEN " + targetCol + " " + lowerBound +" AND " +
					                               targetCol + " " + upperBound + " " 
					 } else if (ruleCond.tToleranceType != null &&
					    (ruleCond.tToleranceType.equalsIgnoreCase("AMOUNT")) ) {
					     
					     var lowerBound : String = ""
					     var upperBound : String = ""
					     
					     if (ruleCond.tToleranceOperatorFrom != null ) {
					        lowerBound = ruleCond.tToleranceOperatorFrom
					     }
					     if (ruleCond.tToleranceValueFrom.contains("%")) {
					        var temp : String = ruleCond.tToleranceValueFrom.replaceAll("%", "")
					        lowerBound = lowerBound + " ((" + sourceCol + " * " + temp + ")/100)" 
					        
					     } else {
					        lowerBound = lowerBound + ruleCond.tToleranceValueFrom
					     }
					     
					     if (ruleCond.tToleranceOperatorTo != null) {
					        upperBound = ruleCond.tToleranceOperatorTo
					     }
					     
					     if (ruleCond.tToleranceValueTo.contains("%")) {
					        var temp : String = ruleCond.tToleranceValueTo.replaceAll("%", "")
					        upperBound = upperBound + " ((" + sourceCol + " * " + temp + ")/100)" 
					        
					     } else {
					        upperBound = upperBound + ruleCond.tToleranceValueTo
					     }					     
					     
					     sourceTagetExpression = " " + targetCol +
					                             " BETWEEN " + sourceCol + " " + lowerBound +" AND " +
					                              sourceCol + " " + upperBound + " " 
					 }
					 
					 if (sourceTagetExpression.length() == 0) {
					    sourceTagetExpression = " " + sourceCol + " " + operator + " " + targetCol + " "
					 }
					 if ( (ruleCond.logicalOperator != null) && 
					      (ruleCond.logicalOperator.equalsIgnoreCase("AND"))) {
					      logicalOperator = "AND"
					 }
					 if ( (ruleCond.logicalOperator != null) && 
					      (ruleCond.logicalOperator.equalsIgnoreCase("OR"))) {
					      logicalOperator = "OR"
					 }
					 
					 whereClause = whereClause + sourceTagetExpression + logicalOperator + " "
       }
       
     }
     if (whereClause.trim().endsWith("OR") ) {
       whereClause = whereClause.substring(0, whereClause.length() - 3);
     }
     if (whereClause.trim().endsWith("AND") ) {
       whereClause = whereClause.substring(0, whereClause.length() - 4);
     }
     whereClause
   }
}