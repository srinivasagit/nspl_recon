package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset,Row,Column}
import scala.collection.mutable.{ArrayBuffer,HashMap}
import java.text.SimpleDateFormat
import java.util.Calendar

class ruleReconciliation {
  
  
  def reconcile (spark: SparkSession, jobId: String, ruleDataRecord :ruleDataViewRecord, 
                 sData: Dataset[Row], tData: Dataset[Row], maxReconReference : Long ) : ArrayBuffer[Dataset[Row]] = {
    
    var reconciledIdsAndStatus : ArrayBuffer[Dataset[Row]] = new ArrayBuffer[Dataset[Row]]()
    
    val ReconUtils = new reconUtils()
    println ("stage-1 : Source data count before recon - " + sData.count())
    println ("stage-1 : Target data count before recon - " + tData.count())
    
    val sourceDataForRecon = ReconUtils.filterSourceData(spark, sData, ruleDataRecord)
    val targetDataForRecon = ReconUtils.filterTargetData(spark, tData, ruleDataRecord)
    
    println ("stage-1 : Source data count after Filtering - " + sourceDataForRecon.count())
    sourceDataForRecon.show()
    
    println ("stage-1 : Target data count after Filtering - " + targetDataForRecon.count())
    targetDataForRecon.show()
   
    var KeyCols : HashMap[String, ArrayBuffer[Column]] = ReconUtils.getGroupByCols(ruleDataRecord)
    
    println("Group By keys : " + KeyCols("Source").mkString(" ") + " - " + KeyCols("Target").mkString(" ") ) 
    
    val filteredSourceDataSet = ReconUtils.filterSourceDataBasedOnRuleType(sourceDataForRecon, KeyCols, ruleDataRecord)
    
    println ("stage-2 : Source data count based one ruleType <" + ruleDataRecord.ruleType +"> :" + filteredSourceDataSet.count)
    filteredSourceDataSet.show()
    
    val filteredTargetDataSet = ReconUtils.filterTargetDataBasedOnRuleType(targetDataForRecon, KeyCols, ruleDataRecord)
    println ("stage-2 : Target data count based one ruleType <" + ruleDataRecord.ruleType +"> :" + filteredTargetDataSet.count)
    filteredTargetDataSet.show()
    
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val processTime = format.format(Calendar.getInstance().getTime()) 
    
    val oneToOne = new OneToOneService ()
    val oneToMany = new OneToManyService()
    val manyToOne = new ManyToOneService()
    val manyToMany = new ManyToManyService()
    
    if (ruleDataRecord.ruleType.equals("ONE_TO_ONE")) {
        println ("Finally we are here for ONE_TO_ONE")
        reconciledIdsAndStatus = oneToOne.reconcileOneToOne(spark, filteredSourceDataSet, filteredTargetDataSet, ruleDataRecord, jobId, maxReconReference, processTime)
    } else if (ruleDataRecord.ruleType.equals("ONE_TO_MANY")) {
        println ("Finally we are here for ONE_TO_MANY")
        reconciledIdsAndStatus = oneToMany.reconcileOneToMany(spark, filteredSourceDataSet, filteredTargetDataSet, targetDataForRecon, ruleDataRecord, jobId, maxReconReference, processTime)
    } else if (ruleDataRecord.ruleType.equals("MANY_TO_ONE")) {
        println ("Finally we are here for MANY_TO_ONE")
        reconciledIdsAndStatus = manyToOne.reconcileManyToOne(spark, filteredSourceDataSet, filteredTargetDataSet, sourceDataForRecon, ruleDataRecord, jobId, maxReconReference, processTime)
    }  else if (ruleDataRecord.ruleType.equals("MANY_TO_MANY")) {
        println ("Finally we are here for MANY_TO_MANY")
        reconciledIdsAndStatus = manyToMany.reconcileManyToMany(spark, filteredSourceDataSet, filteredTargetDataSet, sourceDataForRecon, targetDataForRecon, ruleDataRecord, jobId, maxReconReference, processTime)
    }
    
    reconciledIdsAndStatus
  }
  
  
  
}