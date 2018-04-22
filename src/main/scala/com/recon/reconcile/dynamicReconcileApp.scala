package com.recon.reconcile

import java.util.Calendar 
import java.util.Date
import java.util
import java.util.Properties
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}
import org.apache.spark.sql.{Dataset,Row}
import java.math._;
import org.apache.spark.sql.functions._
//import com.datastax.driver.core.utils.UUIDs

import java.sql.{Connection,Statement,DriverManager}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions


object dynamicReconcile {
  
  def main(args : Array[String]): Unit = {
    
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val processTime = format.format(Calendar.getInstance().getTime()) 
//    val sTime = Calendar.getInstance().getTime()
    
    println ("Dynamic Reconcile - Program started @ %s".format(processTime))
    println ("Dynamic Reconcile - Total no of Arguments %d".format(args.length))
    println ("List of arguments -")
    args.foreach(i => println ("argument - " + i ))
   
    if (args.length != 11 ) {
      println("**************************************")
      println("ERROR : Incorrect No. of parameters")
      println("**************************************")
      System.exit(1)
    }
    
    val DBObj = new DBdetails()

    DBObj.utils(args)
                                    
    println ("Check Database Name : %s".format(DBObj.dbName)) 
    
    val spark : SparkSession = new sparkService().getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "20")
    
    val jobId = spark.sparkContext.applicationId
    println ("Recon application ID " + jobId )
    
    val jdbcMySql = new jdbcScalaMysql()
    val conn: Connection = jdbcMySql.openMysqlConn(DBObj : DBdetails)
    println("Mysql Connection created..") 
    
//    val timeUUID = udf(() => UUIDs.timeBased().toString)
//    spark.sqlContext.udf.register("timeUUID", timeUUID)
    
    if ( DBObj.ruleGroupId != 0L ) {
      println("JDBC Connection to MySQL using Scala - start: %s".format(Calendar.getInstance().getTime()))
      val sTime = Calendar.getInstance().getTime()
      
      val tenantId: Long = jdbcMySql.fetchTenantId(conn : Connection, DBObj : DBdetails).getOrElse(0L)
      
      if (tenantId == 0L) {
        println ("*************************************************")
        println ("ERROR : Tenant is either inactive or not found")
        println ("*************************************************")
        System.exit(1)
      } else {
        println("Tenant id Found = " + DBObj.tenantId)
      }
        
      println("JDBC Connection to MySQL using Scala - End: %s".format(Calendar.getInstance().getTime()))
      
          
      val jdbcSpark = new jdbcSparkMySql()
      
//      jdbcSpark.fetchTenantId(spark : SparkSession)       
//        
//      println("Tenant id Found = " + DBObj.tenantId)
//      println("Time taken for jdbc spark: %s".format(Calendar.getInstance().getTime()))
      
      if (jdbcMySql.checkTenantStatus(conn : Connection, DBObj : DBdetails).getOrElse(false)) {
    
        println ("Tenant is Active")
        
        var ruleConditions = new ArrayBuffer[ruleDataViewRecord]()
            
        ruleConditions = jdbcMySql.retrieveRules(conn : Connection, DBObj : DBdetails)
        
        println (" Rules Count: " + ruleConditions.size)
        
        for (ruleRecord :ruleDataViewRecord  <- ruleConditions) {
          
          println ("Executing Rulename :"              + ruleRecord.ruleType + 
                   " - Rule Id "                       + ruleRecord.ruleId   +    
                   " - started " + Calendar.getInstance().getTime() )

          val reconciliationUtils =  new reconUtils()
          val maxReconRef = jdbcSpark.getMaxReconRef(spark : SparkSession, DBObj : DBdetails).getOrElse(0L)
          
          println ("Executing Rulename :"              + ruleRecord.ruleType + 
                   " - Rule Id :"                      + ruleRecord.ruleId   +    
                   " - Max Reconciliation Reference :" +  maxReconRef )
          
          var dataViewColumnNames : HashMap[String, HashSet[ArrayBuffer[String]]] = reconciliationUtils.fetchDataViewNameColumns(ruleRecord)
       
          println ("DataViewColumnNames :" + dataViewColumnNames)
          
          var ViewAndDataset : HashMap[String, Dataset[Row]] = jdbcSpark.fetchViewAndBaseData(spark : SparkSession, 
                                                                                              ruleRecord : ruleDataViewRecord, 
                                                                                              dataViewColumnNames,
                                                                                              DBObj : DBdetails)
          
          var sourceData: Dataset[Row] = null
          var targetData: Dataset[Row] = null
          
          var sourceVName :String = ruleRecord.sourceViewName
          var targetVName :String = ruleRecord.targetViewName
          
          sourceData = ViewAndDataset.getOrElse(sourceVName, null)
          targetData = ViewAndDataset.getOrElse(targetVName, null)
          
          var reconciledSrcTarIds : ArrayBuffer[Dataset[Row]] = null
          
          if (sourceData.rdd.isEmpty() ) {
            println ("*****Alert***** Source Data is Empty ")
          } else if (targetData.rdd.isEmpty()) {
            println ("*****Alert***** Target Data is Empty ")
          } else {
            
          val RuleReconciliation = new ruleReconciliation()
          reconciledSrcTarIds = RuleReconciliation.reconcile(spark, jobId, ruleRecord, sourceData, targetData, 
                                                              maxReconRef, processTime, DBObj : DBdetails)
          
          println ("Executing Rulename :" + ruleRecord.ruleType  + " - Dataset count : " + reconciledSrcTarIds.size )
          println ("Executing Rulename :" + ruleRecord.ruleType  + " - Ended " + Calendar.getInstance().getTime())
           
          if (reconciledSrcTarIds.size > 0){
            
            println ("Committing result to database : " + ruleRecord.ruleType  + " - Started " + Calendar.getInstance().getTime())
            
            val reconciledResult : Dataset[Row] = reconciledSrcTarIds(0)
            
            val resultToDB = reconciledResult
						                  .withColumn("approval_group_id", functions.lit(null).cast("Long"))
						                  .withColumn("approval_rule_id", functions.lit(null).cast("Long"))
						                  .withColumn("approval_initiation_date", functions.lit(null).cast("Timestamp"))
						                  .withColumn("approval_batch_id", functions.lit(null).cast("Long"))
						                  .withColumn("appr_ref_01", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_02", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_03", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_04", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_05", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_06", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_07", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_08", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_09", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_10", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_11", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_12", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_13", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_14", functions.lit(null).cast("String"))
						                  .withColumn("appr_ref_15", functions.lit(null).cast("String"))
						                  .withColumn("final_status", functions.lit(null).cast("String"))
						                  .withColumn("final_action_date", functions.lit(null).cast("Timestamp"))
            
           resultToDB.show()
           jdbcSpark.writeToDatabase (spark : SparkSession, resultToDB : Dataset[Row], DBObj : DBdetails)
           
           println ("Committing result to database : " + ruleRecord.ruleType  + " - Ended " + Calendar.getInstance().getTime())
           
          }
          
          }
          
        }
      }
      else {
        println("**************************************")
        println("**************************************")
        println("ERROR : Tenant is not active")
        println("**************************************")
        System.exit(1)        
      }
    }
    else {
      println("**************************************")
      println("**************************************")
      println("ERROR : Incorrect Group ID")
      println("**************************************")
      System.exit(1)
    }
    jdbcMySql.closeMysqlConn(conn : Connection)
    spark.stop()
    
    println ("Dynamic Reconcile - Program Ended @ %s".format(Calendar.getInstance().getTime()))
    
    println ("******************************************************************")
    println ("*****                 job successfully completed          ********")
    println ("******************************************************************")
    
    
  }
  
}