package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column

class OneToManyService {
  
  def reconcileOneToMany (spark: SparkSession, filteredSourceDataSet : Dataset[Row],
                          filteredTargetDataSet : Dataset[Row], targetDataForRecon : Dataset[Row], 
                          ruleDataRecord :ruleDataViewRecord,jobId: String,  
                          maxReconReference: Long, processTime: String) : ArrayBuffer[Dataset[Row]] = {
    
     val reconIdsAndStatus = new ArrayBuffer[Dataset[Row]]()
     val ReconcileUtils = new reconUtils()
     var whereClause: String = ReconcileUtils.getWhereClauseFor(ruleDataRecord);     
		 println("stage-3: <ONE_TO_MANY> WhereClause is :" + whereClause);
		 
		 filteredSourceDataSet.createOrReplaceTempView("srcForRecon");
		 filteredTargetDataSet.createOrReplaceTempView("tarForRecon");
		 
		 var reconcileSql: String = "SELECT src.*, tar.* FROM srcForRecon src, tarForRecon tar "
		 		 
		 if (whereClause.length() > 0) {
			  reconcileSql += " WHERE " + whereClause;
		 }
    
		 println ("stage-3: <ONE_TO_MANY> recocile query: " +  reconcileSql)
		 
		 val reconcileResult = new ArrayBuffer[Dataset[Row]]()
     
//		 try {
		  
		   val reconciledJoinOneToM : Dataset[Row] = spark.sql(reconcileSql)
		   
		   if (! reconciledJoinOneToM.take(1).isEmpty) {
		   		   
		      val reconciledSIdJoinInterim : Dataset[Row] = reconciledJoinOneToM.select("scrIds")
		                                                         .withColumnRenamed("scrIds", "original_row_id")
		   
		      val reconciledSIdJoinWithRef = reconciledSIdJoinInterim.withColumn("recon_reference", functions.row_number()
									                                                                                 .over(Window.orderBy("original_row_id"))
                                                                                  								 .plus(maxReconReference))
          val reconciledSIdJoinWithRefFinal = reconciledJoinOneToM.join(reconciledSIdJoinWithRef,
                                                                        reconciledJoinOneToM.col("scrIds")
                                                                        .equalTo(reconciledSIdJoinWithRef.col("original_row_id")))
                                                                  .withColumnRenamed("scrIds", "original_row_id")
       
          val colNames: Array[String] = filteredTargetDataSet.schema.fieldNames.filter(c => c != "SUMTOTAL_TEMP")
          println ("stage-3: Column list other than SUMTOTAL_TEMP -" + colNames.toSeq)
       
          val reconciledTIdJoin = targetDataForRecon.join(reconciledSIdJoinWithRefFinal,colNames.toSeq, "INNER")
			    val reconciledTIdJoinWithRef = reconciledTIdJoin.select("scrIds", "recon_reference")
					                                                .withColumnRenamed("scrIds", "target_row_id")
			    val reconcileSSQL :String = "SELECT null AS id, original_row_id, " +
							 ruleDataRecord.sourceViewId +
							 " AS original_view_id, '' AS original_view, null AS target_row_id, null AS target_view_id, '' AS target_view, " +
							 " recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " + 
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" + 
							 jobId + "' AS recon_job_reference, '" + 
							 processTime + "' AS reconciled_date, " + 
							 DBObj.tenantId + "  AS tenant_id from srcForRecon_12M"
	
			    val reconcileTSQL :String = "SELECT null AS id, null AS original_row_id, null AS original_view_id, '' AS original_view, target_row_id, " +
							 ruleDataRecord.targetViewId + " AS target_view_id, " +
							 " '' AS target_view, recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " +
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" +
							 jobId  			+ "' AS recon_job_reference, '" +
							 processTime  + "' AS reconciled_date, " +
							 DBObj.tenantId	+ " AS tenant_id from tarForRecon_12M"       

			    reconciledSIdJoinWithRef.createOrReplaceTempView("srcForRecon_12M");
  			  reconciledTIdJoinWithRef.createOrReplaceTempView("tarForRecon_12M");
	
	  	  	val reconciledSRef : Dataset[Row] = spark.sql(reconcileSSQL)
		  	  val reconciledTRef : Dataset[Row] = spark.sql(reconcileTSQL)
          val reCount_12M = reconciledSIdJoinWithRef.count() 
			      
          println ("****************************************************************")
   	  	  println ("OneToMany - Reconciled count : " + reCount_12M )
   	  	  println ("****************************************************************")
       		
   	  	  if ( ! reconciledSIdJoinWithRef.take(1).isEmpty) {
   	  	   val reconIdsAndStatus = reconciledSRef.union(reconciledTRef)
   	  	   
   	  	   reconIdsAndStatus.show()
   	  	   
   	  	  }
					                                             
		   }
       
//		 } catch { 
//			      case e: Exception => e.printStackTrace()
//		 }
//		 
//		 if (!reconcileResult.take(1).isEmpty) {
//        reconIdsAndStatus.append(reconcileResult)
//		 }
		 reconIdsAndStatus
  }
}