package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window

class ManyToManyService {
  
  
  def reconcileManyToMany (spark: SparkSession, filteredSourceDataSet : Dataset[Row],
                            filteredTargetDataSet : Dataset[Row], sourceDataForRecon: Dataset[Row], 
                            targetDataForRecon : Dataset[Row],
                            ruleDataRecord :ruleDataViewRecord,jobId: String,  
                            maxReconReference: Long, processTime: String) : ArrayBuffer[Dataset[Row]] = {
    
      val reconIdsAndStatus = new ArrayBuffer[Dataset[Row]]()
      val ReconcileUtils = new reconUtils()
      var whereClause: String = ReconcileUtils.getWhereClauseFor(ruleDataRecord);  
      println("stage-5: <MANY_TO_MANY> WhereClause is :" + whereClause);
    
		  filteredSourceDataSet.createOrReplaceTempView("srcForRecon");
		  filteredTargetDataSet.createOrReplaceTempView("tarForRecon"); 
		  
		  var reconcileSql: String = "SELECT src.*, tar.*,tar.SUMTOTAL_TEMP as tarRef FROM srcForRecon src, tarForRecon tar "
		 		 
		  if (whereClause.length() > 0) {
			   reconcileSql += " WHERE " + whereClause;
		  }		  

 		  println ("stage-5: <MANY_TO_MANY> recocile query: " +  reconcileSql)		 
		 
		  val reconcileResult = new ArrayBuffer[Dataset[Row]]()
		  val reconciledManyToM : Dataset[Row] = spark.sql(reconcileSql)

		  println ("ManyToMany - Join dataset")
		  reconciledManyToM.show()

	    if (! reconciledManyToM.head(1).isEmpty) {
	      
	      	val reconciledIdJoinWithRef = reconciledManyToM.withColumn("recon_reference", functions.row_number()
									                                                                               .over(Window.orderBy("tarRef"))
                                                                                  							 .plus(maxReconReference))
	        val sColNames: Array[String] = filteredSourceDataSet.schema.fieldNames.filter(c => c != "SUMTOTAL_TEMP")
	        val tColNames: Array[String] = filteredTargetDataSet.schema.fieldNames.filter(c => c != "SUMTOTAL_TEMP")
	        
	        val reconciledSIdJoin : Dataset[Row] = sourceDataForRecon.join(reconciledIdJoinWithRef,sColNames.toSeq, "INNER")
			    val reconciledSIdJoinWithRef: Dataset[Row] = reconciledSIdJoin.select("scrIds", "recon_reference")
					                                                              .withColumnRenamed("scrIds", "original_row_id") 
					                                                              
	        val reconciledTIdJoin : Dataset[Row] = targetDataForRecon.join(reconciledIdJoinWithRef,tColNames.toSeq, "INNER")
			    val reconciledTIdJoinWithRef : Dataset[Row] = reconciledTIdJoin.select("scrIds", "recon_reference")
					                                                               .withColumnRenamed("scrIds", "target_row_id")
					                                                               
//			    val reconcileSSQL :String = "SELECT null AS id, original_row_id, " +
          val reconcileSSQL :String = "SELECT original_row_id, " +					                                                               
							 ruleDataRecord.sourceViewId +
							 " AS original_view_id, '' AS original_view, null AS target_row_id, null AS target_view_id, '' AS target_view, " +
							 " recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " + 
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" + 
							 jobId + "' AS recon_job_reference, '" + 
							 processTime + "' AS reconciled_date, " + 
							 DBObj.tenantId + "  AS tenant_id from srcForRecon_M2M"
	
//			    val reconcileTSQL :String = "SELECT null AS id, null AS original_row_id, null AS original_view_id, '' AS original_view, target_row_id, " +
          val reconcileTSQL :String = "SELECT null AS original_row_id, null AS original_view_id, '' AS original_view, target_row_id, " +							 
							 ruleDataRecord.targetViewId + " AS target_view_id, " +
							 " '' AS target_view, recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " +
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" +
							 jobId  			+ "' AS recon_job_reference, '" +
							 processTime  + "' AS reconciled_date, " +
							 DBObj.tenantId	+ " AS tenant_id from tarForRecon_M2M"
							 
			    reconciledSIdJoinWithRef.createOrReplaceTempView("srcForRecon_M2M")
  			  reconciledTIdJoinWithRef.createOrReplaceTempView("tarForRecon_M2M")

  			  val reconciledSRef : Dataset[Row] = spark.sql(reconcileSSQL)
		  	  val reconciledTRef : Dataset[Row] = spark.sql(reconcileTSQL)
//          val reCount_M2M = reconciledManyToM.count()  							 
			      
//          println ("****************************************************************")
//   	  	  println ("ManyToMany - Reconciled count : " + reCount_M2M )
//   	  	  println ("****************************************************************")   

   	  	  if ( ! reconciledIdJoinWithRef.take(1).isEmpty) {
   	  	    
			      println ("ManyToMany  - Source data reconciled count : " + reconciledSRef.count() + 
			                          " - Target data reconciled count : " + reconciledTRef.count())   
			                         
    	  	   val reconIdsAndStatusResult = reconciledSRef.union(reconciledTRef)
    	  	   println ("ManyToMany - reconcile dataset")
        	   reconIdsAndStatusResult.show()
        	   
   	  	     reconIdsAndStatus.append(reconIdsAndStatusResult)
   	  	  }  
          
	    }
    
      reconIdsAndStatus
  }
}