package com.recon.reconcile

import java.util.Calendar 
import java.util.Date
import java.util
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}
import org.apache.spark.sql.{Dataset,Row}

import java.sql.{Connection,Statement,DriverManager}

import org.apache.spark.sql.SparkSession

object dynamicReconcile {
  
  def main(args : Array[String]): Unit = {
    
    val sTime = Calendar.getInstance().getTime()
    
    println ("Dynamic Reconcile - Program started @ %s".format(sTime))
    println ("Dynamic Rimport java.sql.{Connection,Statement,DriverManager}econcile - Total no of Arguments %d".format(args.length))
    println ("List of arguments -")
    args.foreach(i => println ("argument - " + i ))
   
    if (args.length != 11 ) {
      println("**************************************")
      println("**************************************")
      println("ERROR : Incorrect No. of parameters")
      println("**************************************")
      System.exit(1)
    }
    
//    val utilities :utils = new utils(args)

    DBObj.utils(args)
    println ("check variable value: %s".format(DBObj.dbName)) 
    println("Spark context created..") 
    val spark : SparkSession = new sparkService().getSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "25")
    val jobId = spark.sparkContext.applicationId
    println ("NSPL_Recon application ID " + jobId )
    
    val jdbcMySql = new jdbcScalaMysql()
    val conn: Connection = jdbcMySql.openMysqlConn()
    println("Mysql Connection created..") 
    
    if ( DBObj.ruleGroupId != 0L ) {
      println("Time start for jdbc scala: %s".format(Calendar.getInstance().getTime()))
      val sTime = Calendar.getInstance().getTime()
      
      jdbcMySql.fetchTenantId(conn : Connection)
      
      println("Tenant id Found = " + DBObj.tenantId)
      println("Time end for jdbc scala: %s".format(Calendar.getInstance().getTime()))
          
      val jdbcSpark = new jdbcSparkMySql()
//      jdbcSpark.fetchTenantId(spark : SparkSession)       
//        
//      println("Tenant id Found = " + DBObj.tenantId)
//      println("Time taken for jdbc spark: %s".format(Calendar.getInstance().getTime()))
      
      if (jdbcMySql.checkTenantStatus(conn : Connection).get) {
    
        println ("Tenant Active")
        
        var ruleConditions = new ArrayBuffer[ruleDataViewRecord]()
            
        ruleConditions = jdbcMySql.retrieveRules(conn : Connection)
        
        println (" Rules Count: " + ruleConditions.size)
        
        for (ruleRecord :ruleDataViewRecord  <- ruleConditions) {
          
          val reconciliationUtils =  new reconUtils()
        
          val maxReconRef = jdbcMySql.getMaxReconRef(conn : Connection).getOrElse(0L)
          
          var dataViewColumnNames : HashMap[String, HashSet[ArrayBuffer[String]]] = reconciliationUtils.fetchDataViewNameColumns(ruleRecord)
       
          println("DataViewColumnNames :" + dataViewColumnNames)
          
          var ViewAndDataset : HashMap[String, Dataset[Row]] = jdbcSpark.fetchViewAndBaseData(spark : SparkSession, ruleRecord : ruleDataViewRecord, dataViewColumnNames)
          
          var sourceData: Dataset[Row] = null
          var targetData: Dataset[Row] = null
          
          var sourceVName :String = ruleRecord.sourceViewName
          var targetVName :String = ruleRecord.targetViewName
          
          sourceData = ViewAndDataset.getOrElse(sourceVName, null)
          targetData = ViewAndDataset.getOrElse(targetVName, null)
          
          println ("Executing Rule : " + ruleRecord.ruleType  + " - started " + Calendar.getInstance().getTime())
          
          var reconciledSrcTarIds : ArrayBuffer[Dataset[Row]] = null
          
          if (sourceData.rdd.isEmpty() ) {
            println ("*****Alert***** Source Data is Empty ")
          } else if (targetData.rdd.isEmpty()) {
            println ("*****Alert***** Target Data is Empty ")
          } else {
            
          val RuleReconciliation = new ruleReconciliation()
          reconciledSrcTarIds = RuleReconciliation.reconcile(spark, jobId, ruleRecord, sourceData, targetData, maxReconRef)
          
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
    
    println ("Dynamic Reconcile - Program started @ %s".format(Calendar.getInstance().getTime()))
    
    println ("******************************************************************")
    println ("*****                 job successfully completed          ********")
    println ("******************************************************************")
    
    
  }
  
}