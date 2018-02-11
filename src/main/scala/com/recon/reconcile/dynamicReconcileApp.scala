package com.recon.reconcile

import java.util.Calendar 
import java.util.Date
import java.util
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{HashMap,HashSet}

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
    val job = spark.sparkContext.applicationId
    println ("NSPL_Recon application ID " + job )
    
    val jdbcMySql = new jdbcScalaMysql()
    val conn: Connection = jdbcMySql.openMysqlConn()
    println("Mysql Connection created..") 
    
    if ( DBObj.ruleGroupId != 0L ) {
      println ("time start for jdbc scala: %s".format(Calendar.getInstance().getTime()))
      val sTime = Calendar.getInstance().getTime()
      
      jdbcMySql.fetchTenantId(conn : Connection)
      
      println("Tenant id Found = " + DBObj.tenantId)
      println ("time end for jdbc scala: %s".format(Calendar.getInstance().getTime()))
          
      val jdbcSpark = new jdbcSparkMySql()
      jdbcSpark.fetchTenantId(spark : SparkSession)       
        
      println("Tenant id Found = " + DBObj.tenantId)
      println ("time taken for jdbc spark: %s".format(Calendar.getInstance().getTime()))
      
      if (jdbcMySql.checkTenantStatus(conn : Connection).get) {
        println ("Tenant Active")
        
        var ruleConditions = new ArrayBuffer[ruleDataViewRecord]()
        
        ruleConditions = jdbcMySql.retrieveRules(conn : Connection)
        
        println (" Rules Count: " + ruleConditions.size)
        
        for ( ruleRecord :ruleDataViewRecord  <- ruleConditions) {
          
          val reconciliationUtils =  new reconUtils()
          
          var dataViewColumnNames : HashMap[String, HashSet[ArrayBuffer[String]]] = reconciliationUtils.fetchDataViewNameColumns(ruleRecord)
       
          println("dataViewColumnNames :" + dataViewColumnNames)
          
          var ViewAndColumn = jdbcSpark.fetchViewAndBaseData(spark : SparkSession, ruleRecord : ruleDataViewRecord, dataViewColumnNames)
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
  }
  
}