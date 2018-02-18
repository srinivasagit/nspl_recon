package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}
import org.apache.spark.sql.Column

class jdbcSparkMySql {
  
  
  def fetchTenantId(spark: SparkSession) : Unit = {
		try {	
  			val driver = "com.mysql.jdbc.Driver"
  			val username = DBObj.dbUser
  			val password = DBObj.dbPass
  			
  			println("Mysql connection : %s".format(DBObj.mySqlUrl))
  			println("Mysql username : " + DBObj.dbUser)
  			println("Mysql password : " + DBObj.dbPass )
  					
  			val table = "t_rule_group_details"
        val predicate = "rule_group_id = " + DBObj.ruleGroupId
        
        DBObj.tenantId = spark.read.jdbc(DBObj.mySqlUrl, table, DBObj.buildProps())
                                   .where(predicate)
                                   .select("tenant_id")
                                   .collectAsList().get(0).getLong(0)
			} catch {
			      case e: Exception => e.printStackTrace()
			}
  }
  
  def fetchViewAndBaseData (spark : SparkSession, 
                            ruleDataRecord :ruleDataViewRecord,  
                            viewColumnNames : HashMap[String, HashSet[ArrayBuffer[String]]]) : 
                            HashMap[String, Dataset[Row]] = {
    
      val reconUtilities =  new reconUtils()
      val viewWithBaseData : HashMap[String, Dataset[Row]] = HashMap.empty 
      
      val ruleID: Long = ruleDataRecord.ruleId
      val sourceViewName : String =ruleDataRecord.sourceViewName
      val sourceViewID : Long = ruleDataRecord.sourceViewId
      val targetViewName : String = ruleDataRecord.targetViewName
      val targetViewID : Long = ruleDataRecord.targetViewId
      
      val sourceViewColumn : HashSet[ArrayBuffer[String]] = viewColumnNames.getOrElse(sourceViewName, HashSet.empty)
      val targetViewColumn : HashSet[ArrayBuffer[String]] = viewColumnNames.getOrElse(targetViewName, HashSet.empty)
      
//      val selectSourceSQL : String = reconUtilities.getSelSqlWithAliasAndCast(sourceViewColumn)
//      
//      val selectTargetSQL : String = reconUtilities.getSelSqlWithAliasAndCast(targetViewColumn)
      
      val selectSourceSQL : ArrayBuffer[Column] = reconUtilities.getSelSqlWithAliasAndCast(sourceViewColumn)
      val selectTargetSQL : ArrayBuffer[Column] = reconUtilities.getSelSqlWithAliasAndCast(targetViewColumn)
      
      println ("ruleid  : "  + ruleID)
      println ("SourceViewName : " + sourceViewName + " Id : " + sourceViewID)
      println ("targetViewName : " + targetViewName + " Id : " + targetViewID)      
      println ("source SQL :" + selectSourceSQL)
      println ("target SQL :" + selectTargetSQL)
      
      var sourceViewData : Dataset[Row] = null
      var targetViewData : Dataset[Row] = null
      var reconciledSIds : Dataset[Row] = null
      var reconciledTIds : Dataset[Row] = null
      
      val table_reconciled : String = "t_reconciliation_result"
      val predicate_s :String  = " original_view_id = " + sourceViewID
      val predicate_t :String  = " original_view_id = " + targetViewID
      
      sourceViewData = spark.read.jdbc(DBObj.mySqlUrl, sourceViewName.toLowerCase(), DBObj.buildProps())
      
//      sourceViewData.createOrReplaceTempView("sourceViewData_temp")
      
      val sourceViewDataFinal = sourceViewData.selectExpr(selectSourceSQL.map(r => r.toString): _*)
      
//      val sourceViewDataFinal = spark.sql("SELECT " + selectSourceSQL + " FROM sourceViewData_temp")
      
      println ("stage-0 : Source record count : "   + sourceViewDataFinal.count())
      
      reconciledSIds = spark.read.jdbc(DBObj.mySqlUrl, table_reconciled.toLowerCase(), DBObj.buildProps())
                                 .where(predicate_s)
                                 .select("original_row_id")
                                 .withColumnRenamed("original_row_id", "scrIds")
      
      println ("stage-0 : Source record count @ table_reconciled : "   + reconciledSIds.count())
      
      val sourceViewDataFiltered = sourceViewDataFinal.join(reconciledSIds,Seq("scrIds"), "leftanti")     
      
      sourceViewDataFiltered.show()
      
      println ("stage-0 : Source record count excluding already reconciled: "   + sourceViewDataFiltered.count())
      
      targetViewData = spark.read.jdbc(DBObj.mySqlUrl, targetViewName.toLowerCase(), DBObj.buildProps())
      
//      targetViewData.createOrReplaceTempView("targetViewData_temp")
      
      val targetViewDataFinal = targetViewData.selectExpr(selectTargetSQL.map(r => r.toString): _*)
      
//      val targetViewDataFinal = spark.sql ( "SELECT " + selectTargetSQL + " FROM targetViewData_temp")
      
      println ("stage-0 : Target record count : "   + targetViewDataFinal.count())
      
      reconciledTIds = spark.read.jdbc(DBObj.mySqlUrl, table_reconciled.toLowerCase(), DBObj.buildProps())
                                 .where(predicate_t)
                                 .select("original_row_id")
                                 .withColumnRenamed("original_row_id", "scrIds")
      
      println ("stage-0 : Target record count @ table_reconciled : "   + reconciledTIds.count())
      
      val targetViewDataFiltered = targetViewDataFinal.join(reconciledTIds,Seq("scrIds"), "leftanti")     
      
      targetViewDataFiltered.show()
      
      println ("stage-0 : Target record count excluding already reconciled : " + targetViewDataFiltered.count())      
      
      viewWithBaseData.put(sourceViewName, sourceViewDataFiltered)
      viewWithBaseData.put(targetViewName, targetViewDataFiltered)
      viewWithBaseData
  }
}