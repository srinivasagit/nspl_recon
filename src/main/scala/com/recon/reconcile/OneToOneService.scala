package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset,Row}
import scala.collection.mutable.{ArrayBuffer,HashMap}
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window

class OneToOneService {
  
   def reconcileOneToOne (spark: SparkSession, filteredSourceDataSet : Dataset[Row],
                          filteredTargetDataSet  : Dataset[Row], ruleDataRecord :ruleDataViewRecord, 
                          jobId: String, maxReconReference: Long, processTime: String) : ArrayBuffer[Dataset[Row]] = {
     
     var reconIdsAndStatus = new ArrayBuffer[Dataset[Row]] ()
     val ReconcileUtils = new reconUtils()
     var whereClause: String = ReconcileUtils.getWhereClauseFor(ruleDataRecord);     
		 println("WhereClause is :" + whereClause);
		 
		 var reconcileSql: String = "select src.scrIds as original_row_id, tar.scrIds as target_row_id from  srcForRecon as src, tarForRecon as tar "
     
		 println ("ONE_TO_ONE query : " + reconcileSql) 
		 
		 var reconcileSSQL = "select  original_row_id, " + 
		     ruleDataRecord.sourceViewId + " as original_view_id, '' as original_view, null as target_row_id, null as target_view_id, '' as target_view, " +
				 " recon_reference, '' as reconciliation_rule_name, " + 
				 DBObj.ruleGroupId+ " as reconciliation_rule_group_id, " +
				 ruleDataRecord.ruleId + " as reconciliation_rule_id, '" +
				 DBObj.userId + "' as reconciliation_user_id, '" + 
				 jobId + "' as recon_job_reference, '" + 
				 processTime + "' as reconciled_date, " + 
				 DBObj.tenantId + " as tenant_id"
		 
		 var reconcileTSQL = "select null as original_row_id, null as original_view_id, '' as original_view, target_row_id, " +
				 ruleDataRecord.targetViewId + " as target_view_id, " +
				 " '' as target_view, recon_reference, '' as reconciliation_rule_name, " + 
				 DBObj.ruleGroupId + " as reconciliation_rule_group_id, " + 
				 ruleDataRecord.ruleId	+ " as reconciliation_rule_id, '" +
				 DBObj.userId + "' as reconciliation_user_id, '"	+ 
				 jobId	+ "' as recon_job_reference, '" +
				 processTime	+ "' as reconciled_date, " + DBObj.tenantId + " as tenant_id"
		 
		 println (" reconcileSSQL :" + reconcileSSQL)
		 println (" reconcileTSQL :" + reconcileTSQL)
		 
		 if (whereClause.length() > 0) {
			reconcileSql += " where " + whereClause;
		 }
		 
		 filteredSourceDataSet.createOrReplaceTempView("srcForRecon");
		 filteredTargetDataSet.createOrReplaceTempView("tarForRecon");
		 
		 var reconcileSQLs : Array[String] = new Array[String](3)
		 reconcileSQLs(0) = reconcileSql
		 reconcileSQLs(1) = reconcileSSQL
		 reconcileSQLs(2) = reconcileTSQL
		 
		 reconIdsAndStatus = executeReconAndUpdate(spark, reconcileSQLs, ruleDataRecord, maxReconReference)
		 
		 reconIdsAndStatus
   }
   
   
    def executeReconAndUpdate (spark: SparkSession, reconcileSQLs: Array[String],
                               ruleDataRecord :ruleDataViewRecord, ReconReference: Long) : ArrayBuffer[Dataset[Row]] = {
      
      var reconciled : Dataset[Row] = null
      var reconIdsAndStatusArray : ArrayBuffer[Dataset[Row]] = new ArrayBuffer[Dataset[Row]]()
      
      reconciled = spark.sql(reconcileSQLs(0).toString());
      
   		reconciled = reconciled
   		             .withColumn("recon_reference", functions.row_number().over(Window.orderBy("original_row_id"))
						       .plus(ReconReference))
						       
		  reconciled.createOrReplaceTempView("treconciliationresult")
		  
		  var reconciledSSql : String = reconcileSQLs(1) + " from treconciliationresult"
		  var reconciledTSql : String = reconcileSQLs(2) + " from treconciliationresult"
      
		  var reconciledS : Dataset[Row] = null
      var reconciledT : Dataset[Row] = null
      var reconciledWithId : Dataset[Row] = null
      var recCount: Long = reconciled.count()
      
      println ("****************************************************************")
   		println ("OneToOne - Reconciled count : " + recCount )
   		println ("****************************************************************")
      
   		reconciled.show()
      reconciled.explain()
      reconciled.queryExecution
      
      if (! reconciled.head(1).isEmpty) {
        
			    reconciledS  = spark.sql(reconciledSSql)
			    reconciledT  = spark.sql(reconciledTSql)
		
			    reconciledWithId = reconciledS.union(reconciledT)
			                                  .withColumn("id",functions.row_number().over(Window.orderBy("original_row_id")).plus(ReconReference))          
			    reconIdsAndStatusArray.append(reconciledWithId)
			    reconciledWithId.show()
      }
		  
      reconIdsAndStatusArray
    }
  
}