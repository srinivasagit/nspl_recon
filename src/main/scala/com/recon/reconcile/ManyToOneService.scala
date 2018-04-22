package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//import com.datastax.driver.core.utils.UUIDs

class ManyToOneService {
  
    def reconcileManyToOne (spark: SparkSession, filteredSourceDataSet : Dataset[Row],
                            filteredTargetDataSet : Dataset[Row], sourceDataForRecon: Dataset[Row],  
                            ruleDataRecord :ruleDataViewRecord,jobId: String,  
                            maxReconReference: Long, processTime: String,
                            DBObj : DBdetails) : ArrayBuffer[Dataset[Row]] = {
      
      val reconIdsAndStatus = new ArrayBuffer[Dataset[Row]]()
      val ReconcileUtils = new reconUtils()
      var whereClause: String = ReconcileUtils.getWhereClauseFor(ruleDataRecord);  
      println("stage-4: <MANY_TO_ONE> WhereClause is :" + whereClause);
		 
		  filteredSourceDataSet.createOrReplaceTempView("srcForRecon");
		  filteredTargetDataSet.createOrReplaceTempView("tarForRecon");      
      
		  var reconcileSql: String = "SELECT src.*, tar.* FROM srcForRecon src, tarForRecon tar "
		 		 
		  if (whereClause.length() > 0) {
			   reconcileSql += " WHERE " + whereClause;
		  }		  

 		  println ("stage-4: <MANY_TO_ONE> recocile query: " +  reconcileSql)
		 
		  val reconcileResult = new ArrayBuffer[Dataset[Row]]()
		  
		  val reconciledManyToO : Dataset[Row] = spark.sql(reconcileSql)
		  
		  println ("ManyToOne - Join dataset")
		  
		  reconciledManyToO.show()
		   
		  if (! reconciledManyToO.head(1).isEmpty) {	
		    
//		      val timeUUID = udf(() => UUIDs.timeBased().toString)
		      val reconciledManyToOInterim : Dataset[Row] = reconciledManyToO.select("scrIds")
		                                                         .withColumnRenamed("scrIds", "target_row_id")
		                                                         
//		      val reconciledTIdJoinWithRef = reconciledManyToOInterim.withColumn("recon_reference",timeUUID())

		      val reconciledTIdJoinWithRef = reconciledManyToOInterim.withColumn("recon_reference", functions.row_number()
									                                                                                 .over(Window.orderBy("target_row_id"))
                                                                                  								 .plus(maxReconReference))		

          val reconciledTIdJoinWithRefFinal = reconciledManyToO.join(reconciledTIdJoinWithRef,
                                                                        reconciledManyToO.col("scrIds")
                                                                        .equalTo(reconciledTIdJoinWithRef.col("target_row_id")))
                                                               .withColumnRenamed("scrIds", "target_row_id")                                                                                  								 

          val colNames: Array[String] = filteredSourceDataSet.schema.fieldNames.filter(c => c != "SUMTOTAL_TEMP")
          println ("stage-4: Column list other than SUMTOTAL_TEMP -" + colNames.toSeq)     

          val reconciledSIdJoin = sourceDataForRecon.join(reconciledTIdJoinWithRefFinal,colNames.toSeq, "INNER")
			    val reconciledSIdJoinWithRef = reconciledSIdJoin.select("scrIds", "recon_reference")
					                                                .withColumnRenamed("scrIds", "original_row_id")          

			    val reconcileSSQL :String = "SELECT original_row_id, " +
							 ruleDataRecord.sourceViewId +
							 " AS original_view_id, '' AS original_view, null AS target_row_id, null AS target_view_id, '' AS target_view, " +
							 " recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " + 
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" + 
							 jobId + "' AS recon_job_reference, '" + 
							 processTime + "' AS reconciled_date, " + 
							 DBObj.tenantId + "  AS tenant_id from srcForRecon_M21"
	
			    val reconcileTSQL :String = "SELECT null AS original_row_id, null AS original_view_id, '' AS original_view, target_row_id, " +
							 ruleDataRecord.targetViewId + " AS target_view_id, " +
							 " '' AS target_view, recon_reference, '' AS reconciliation_rule_name, " + 
							 DBObj.ruleGroupId + " AS reconciliation_rule_group_id, " +
							 ruleDataRecord.ruleId + " AS reconciliation_rule_id, '" + DBObj.userId + "' AS reconciliation_user_id, '" +
							 jobId  			+ "' AS recon_job_reference, '" +
							 processTime  + "' AS reconciled_date, " +
							 DBObj.tenantId	+ " AS tenant_id from tarForRecon_M21"
							 
			    reconciledSIdJoinWithRef.createOrReplaceTempView("srcForRecon_M21");
  			  reconciledTIdJoinWithRef.createOrReplaceTempView("tarForRecon_M21");
  			  
	  	  	val reconciledSRef : Dataset[Row] = spark.sql(reconcileSSQL)
		  	  val reconciledTRef : Dataset[Row] = spark.sql(reconcileTSQL)
//		  	  println("ManyToOne - Join Dataset")
//		  	  reconciledTIdJoinWithRef.show()
//          val reCount_M21 = reconciledTIdJoinWithRef.count()  
//			      
//          println ("****************************************************************")
//   	  	  println ("ManyToOne - Reconciled count : " + reCount_M21 )
//   	  	  println ("****************************************************************")   

   	  	  if ( ! reconciledTIdJoinWithRef.take(1).isEmpty) {
   	  	    
			      println ("ManyToOne  - Source data reconciled count : " + reconciledSRef.count() + 
			                         " - Target data reconciled count : " + reconciledTRef.count())   	  	    
   	  	       	  	    
   	  	    val reconIdsAndStatusResult = reconciledSRef.union(reconciledTRef)
   	  	                                                //.withColumn("id",functions.row_number().over(Window.orderBy("target_row_id")).plus(maxReconReference))
//   	  	   	println ("ManyToOne - reconcile dataset")
//   	  	    reconIdsAndStatusResult.show()
   	  	    reconIdsAndStatus.append(reconIdsAndStatusResult)
   	  	  }   	  	  
					                                                
		  }
		  
		  reconIdsAndStatus
    }
  
}