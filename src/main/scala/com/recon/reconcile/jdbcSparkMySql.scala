package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}
import org.apache.spark.sql.Column
import com.mysql.jdbc.Connection
import org.apache.spark.util.SizeEstimator


class jdbcSparkMySql {
  
  
//  def fetchTenantId(spark: SparkSession) : Unit = {
//		try {	
//  			val driver = "com.mysql.jdbc.Driver"
//  			val username = DBObj.dbUser
//  			val password = DBObj.dbPass
//  			
//  			println("Mysql connection : %s".format(DBObj.mySqlUrl))
//  			println("Mysql username : " + DBObj.dbUser)
//  			println("Mysql password : " + DBObj.dbPass )
//  					
//  			val table = "t_rule_group_details"
//        val predicate = "rule_group_id = " + DBObj.ruleGroupId
//        
//        DBObj.tenantId = spark.read.jdbc(DBObj.mySqlUrl, table, DBObj.buildProps())
//                                   .where(predicate)
//                                   .select("tenant_id")
//                                   .collectAsList().get(0).getLong(0)
//			} catch {
//			      case e: Exception => e.printStackTrace()
//			}
//  }
  
  	def getMaxReconRef (spark : SparkSession, DBObj : DBdetails) : Option[Long] = {
	   
	   try {
	         val queryFetchMaxReconRef = "(SELECT max(CONVERT(recon_reference,UNSIGNED INTEGER)) as recon_reference FROM t_reconciliation_result) as k"
	  	     println ("Recon Ref Query:" + queryFetchMaxReconRef)
	  	     
//	  	     spark.read.jdbc(DBObj.mySqlUrl, sourceViewName.toLowerCase(), DBObj.buildProps()).
	  	     var maxRefRecon : Long = 0L
//	  	     maxRefRecon = spark.read.jdbc(DBObj.mySqlUrl, queryFetchMaxReconRef, DBObj.buildProps())
	  	     maxRefRecon = spark.read.jdbc(DBObj.target_mySqlUrl, queryFetchMaxReconRef, DBObj.buildProps())
	  	                             .collectAsList().get(0).getDecimal(0).longValue()
	  	                             
//	         val statement = connection.createStatement
//		       val rs = statement.executeQuery(queryFetchMaxReconRef)
//						while (rs.next) {
//						     if (rs.getString("recon_reference") != null) {
//						         maxRefRecon = rs.getString("recon_reference").toLong
//						     }
//						}
//			      rs.close()
//	          println ("Max RefCon : " + maxRefRecon) 
	          Some(maxRefRecon)
	    } catch {
		      case e: Exception => { e.printStackTrace(); None}
  		}
	}
  
  def writeToDatabase (spark : SparkSession, resultToDB: Dataset[Row], DBObj : DBdetails) : Unit = {

    try {
        val table_reconciled : String = "t_reconciliation_result"
//        println ("JDBC properties :" + DBObj.buildProps() + " url :" + DBObj.mySqlUrl)
//        resultToDB.write.mode("APPEND").jdbc(DBObj.mySqlUrl, table_reconciled, DBObj.buildProps())
        resultToDB.write.mode("APPEND").jdbc(DBObj.target_mySqlUrl, table_reconciled, DBObj.buildProps())
    
        } catch {
		      case e: Exception => { e.printStackTrace(); None}
    }
  }
  	
  def fetchViewAndBaseData (spark : SparkSession, 
                            ruleDataRecord :ruleDataViewRecord,  
                            viewColumnNames : HashMap[String, HashSet[ArrayBuffer[String]]], DBObj : DBdetails) : 
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
      val predicate_s :String  = " t.original_view_id = " + sourceViewID
      val predicate_t :String  = " t.target_view_id = " + targetViewID
      
      val srcPushDownQuery = "( SELECT  s.* FROM " + sourceViewName.toLowerCase() + " AS s " +
                                " WHERE s.scrIds NOT in (SELECT t.original_row_id as scrIds From "+ DBObj.target_dbName + ".t_reconciliation_result t WHERE " + 
                                 predicate_s + " ) ) srcRecFilter"
                                
      val tarPushDownQuery = "( SELECT tar.* FROM " + targetViewName.toLowerCase() + " AS tar " +  
                                " WHERE tar.scrIds NOT in (SELECT t.target_row_id as scrIds From "+ DBObj.target_dbName + ".t_reconciliation_result t WHERE " + 
                                predicate_t + " ) ) tarRecFilter"
                                
      println( "Source pushdown query :")
      println (srcPushDownQuery)

      println( "Target pushdown query :")
      println (tarPushDownQuery)
      
      val sourceViewDataFiltered =  spark.read.jdbc(DBObj.mySqlUrl, srcPushDownQuery , DBObj.buildProps())
                                              .selectExpr(selectSourceSQL.map(r => r.toString): _*)
//                                              .repartition(4)
      
//      sourceViewData = spark.read.jdbc(DBObj.mySqlUrl, sourceViewName.toLowerCase(), DBObj.buildProps())
//      
//      val sourceViewDataFinal = sourceViewData.selectExpr(selectSourceSQL.map(r => r.toString): _*)
//      
//      println ("stage-0 : Source record count : "   + sourceViewDataFinal.count())
//      
//      reconciledSIds = spark.read.jdbc(DBObj.mySqlUrl, table_reconciled.toLowerCase(), DBObj.buildProps())
//                                 .where(predicate_s)
//                                 .select("original_row_id")
//                                 .withColumnRenamed("original_row_id", "scrIds")
//      
//      println ("stage-0 : Source record count @ table_reconciled : "   + reconciledSIds.count())
//      
//      val sourceViewDataFiltered = sourceViewDataFinal.join(reconciledSIds,Seq("scrIds"), "leftanti")     
//    
      println ("stage-0 : Source record count to be reconciled : "   + sourceViewDataFiltered.count() +
               " - Partition count : " + sourceViewDataFiltered.rdd.partitions.size ) 
//               " - data size : " + SizeEstimator.estimate(sourceViewDataFiltered))
//      sourceViewDataFiltered.show()
      
      val targetViewDataFiltered =  spark.read.jdbc(DBObj.mySqlUrl, tarPushDownQuery , DBObj.buildProps())
                                              .selectExpr(selectTargetSQL.map(r => r.toString): _*)
//                                              .repartition(4)
                                              
//      targetViewData = spark.read.jdbc(DBObj.mySqlUrl, targetViewName.toLowerCase(), DBObj.buildProps())
//      
//      val targetViewDataFinal = targetViewData.selectExpr(selectTargetSQL.map(r => r.toString): _*)
//      
//      println ("stage-0 : Target record count : "   + targetViewDataFinal.count())
//      
//      reconciledTIds = spark.read.jdbc(DBObj.mySqlUrl, table_reconciled.toLowerCase(), DBObj.buildProps())
//                                 .where(predicate_t)
//                                 .select("target_row_id")
//                                 .withColumnRenamed("target_row_id", "scrIds")
//      
//      println ("stage-0 : Target record count @ table_reconciled : "   + reconciledTIds.count())
//      
//      val targetViewDataFiltered = targetViewDataFinal.join(reconciledTIds,Seq("scrIds"), "leftanti")     
      
      println ("stage-0 : Target record count to be reconciled : " + targetViewDataFiltered.count() +
               " - Partition count : " + targetViewDataFiltered.rdd.partitions.size ) 
//               " - data size : " + SizeEstimator.estimate(sourceViewDataFiltered))      
//      targetViewDataFiltered.show()
      
      viewWithBaseData.put(sourceViewName, sourceViewDataFiltered)
      viewWithBaseData.put(targetViewName, targetViewDataFiltered)
      viewWithBaseData
  }
}