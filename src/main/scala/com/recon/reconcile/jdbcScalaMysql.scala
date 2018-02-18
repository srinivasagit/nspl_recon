package com.recon.reconcile

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import java.sql.{Connection,Statement,DriverManager}

class jdbcScalaMysql {

  def openMysqlConn() : Connection = { 

      val driver = "com.mysql.jdbc.Driver"
	    val username = DBObj.dbUser
	    val password = DBObj.dbPass
	    println("Mysql connection : %s".format(DBObj.mySqlUrl))
	    println("Mysql username : " + DBObj.dbUser)
	    println("Mysql password : " + DBObj.dbPass )

	    Class.forName(driver)
			DriverManager.getConnection(DBObj.mySqlUrl, DBObj.dbUser, DBObj.dbPass)
  
  }
  
	def fetchTenantId(connection: Connection) : Unit = {
			
			try {	
			      val queryFetchTenantId = "SELECT tenant_id FROM " +  DBObj.dbName + ".t_rule_group_details WHERE rule_group_id = " + DBObj.ruleGroupId + " LIMIT 1"
			      val statement = connection.createStatement
						val rs = statement.executeQuery(queryFetchTenantId)
						
						while (rs.next) {
						     DBObj.setTenantId(rs.getLong("tenant_id"))
						}
			      rs.close()
			} catch {
			      case e: Exception => e.printStackTrace()
			} 
	}
	
	def getMaxReconRef (connection : Connection) : Option[Long] = {
	   
	   try {
	         val queryFetchMaxReconRef = "SELECT max(CONVERT(recon_reference,UNSIGNED INTEGER)) as recon_reference FROM " +  DBObj.dbName + ".t_reconciliation_result"
	  	     println ("Recon Ref Query:" + queryFetchMaxReconRef)
	         val statement = connection.createStatement
		       val rs = statement.executeQuery(queryFetchMaxReconRef)
		       var maxRefRecon : Long = 0L
		     
						while (rs.next) {
						     if (rs.getString("recon_reference") != null) {
						         maxRefRecon = rs.getString("recon_reference").toLong
						     }
						}
			      rs.close()
	          println ("Max RefCon : " + maxRefRecon) 
	          Some(maxRefRecon)
	    } catch {
		      case e: Exception => { e.printStackTrace(); None}
  		}
	}
	
	def checkTenantStatus(connection: Connection): Option[Boolean] = {
			try {	
			      val result : Boolean = true
						val queryCheckTenantId = "SELECT contract_num FROM " +  DBObj.dbName + 
						                         ".t_tenant_config_modules WHERE tenant_id = " + DBObj.tenantId + 
						                         " AND modules = '" + DBObj.module + "' AND enabled_flag = true AND " + 
						                         " (current_date() Between start_date AND (case when end_date is null then date_add(current_date(),INTERVAL 2 DAY) else end_date end))"
						
						val statement = connection.createStatement
						val rs = statement.executeQuery(queryCheckTenantId)
						
						while (rs.next) {
						     DBObj.setContract_num (rs.getLong("contract_num"))
						     print ("Tenant contract id: " + DBObj.contract_num )
						}
			      rs.close()
			      if ( DBObj.contract_num != 0L ){
			        Some (result)
			      } else {
			        val result : Boolean = false
			        Some (result)
			      }
			      
				} catch {
			      case e: Exception => { e.printStackTrace(); None; }
			}  
	  
	}
	
	def retrieveRules(connection: Connection) : ArrayBuffer[ruleDataViewRecord] = {
	  var rulesList = new ArrayBuffer[ruleDataViewRecord]()
	  val queryRules = "SELECT id FROM " +  DBObj.dbName + 
	                   ".t_rule_group WHERE id = " + DBObj.ruleGroupId + 
	                   " AND enabled_flag = true AND " + 
	                   " (current_date() Between start_date AND (case when end_date is null then date_add(current_date(),INTERVAL 2 DAY) else end_date end))"
		var stmt = connection.createStatement
		val rs = stmt.executeQuery(queryRules)

		var rowCount : Int = 0
	  while (rs.next) {
	       rowCount+= 1
		     println ("Group Id : " + rs.getLong("id"))
		}
	  rs.close()
	  println ("no of records : " + rowCount)
	  if (rowCount != 1 ) {
	    println("**************************************")
      println("**************************************")
      println("ERROR : Incorrect Group ID count ")
      println("**************************************")
      System.exit(1)
	  }
	  
	  stmt = connection.createStatement
	  val queryRuleCond = "SELECT rule_id, priority FROM " +  DBObj.dbName +
	                      ".t_rule_group_details  WHERE rule_group_id = " + DBObj.ruleGroupId +
	                      " AND enabled_flag = true ORDER BY priority "

	  println ("query : " + queryRuleCond )
	  
	  val rs2 = stmt.executeQuery(queryRuleCond)
	  rowCount = 0
	  
	  while (rs2.next) {
	       rowCount+= 1
		     println ("Group Id : " + DBObj.ruleGroupId + 
                  " - ruleId : " + rs2.getLong("rule_id") +
                  " - priority : " + rs2.getInt ("priority"))
		
	       var ruleRecord = new ruleDataViewRecord()
	       var ruleId = rs2.getLong("rule_id")
	       var prioritY = rs2.getInt ("priority")
	       
	       ruleRecord.ruleId_=(ruleId)  //setter
	       ruleRecord.priority_=(prioritY) //setter
	       
	       stmt = connection.createStatement
	       
	       val queryRuleType = "SELECT rule_type, source_data_view_id, target_data_view_id, rule_code FROM " +  DBObj.dbName +
	                      ".t_rules  WHERE id = " + ruleId +
	                      " AND enabled_flag = true  AND " +
	                      " (current_date() Between start_date AND (case when end_date is null then date_add(current_date(),INTERVAL 2 DAY) else end_date end))"
	       
	       println ("query : " + queryRuleType )     
	       
	       val rs3 = stmt.executeQuery(queryRuleType)
	       
	       while (rs3.next()) {
	         
	         println ("rule_type : " + rs3.getString("rule_type") +
	                  " - source dataview id : " + rs3.getLong("source_data_view_id") +
	                  " - target dataview id : " + rs3.getLong("target_data_view_id") +
	                  " - rule_code : " + rs3.getString("rule_code"))
	        
	         var rule_Type = rs3.getString("rule_type")
	         var source_View_Id = rs3.getLong("source_data_view_id")
	         var target_View_Id = rs3.getLong("target_data_view_id")
	         var rule_Code = rs3.getString("rule_code")
	         var source_View_Name :String = ""
	         var target_View_Name :String = ""
	         
	         stmt = connection.createStatement
	         
	         val queryRuleView = "SELECT id, data_view_name FROM " + DBObj.dbName +
	                             ".t_data_views WHERE id in (" +  source_View_Id + "," + target_View_Id + ")" +
	                             " AND enabled_flag = true  AND " +
	                             " (current_date() Between start_date AND (case when end_date is null then date_add(current_date(),INTERVAL 2 DAY) else end_date end))"           
	                  
	         println ("query : " + queryRuleView )     
	       
	         val rs4 = stmt.executeQuery(queryRuleView)
	         
	         while (rs4.next()) {
	           
	           if (rs4.getLong("id") ==  source_View_Id ) {
	             source_View_Name = rs4.getString("data_view_name")
	           } else if (rs4.getLong("id") ==  target_View_Id ) {
	             target_View_Name = rs4.getString("data_view_name")
	           }
	         }
	         
	         ruleRecord.ruleType_=(rule_Type)            //setter
	         ruleRecord.sourceViewId_=(source_View_Id)
	         ruleRecord.targetViewId_=(target_View_Id)
	         ruleRecord.sourceViewName_=(source_View_Name)
	         ruleRecord.targetViewName_=(target_View_Name)
	         ruleRecord.ruleName_=(rule_Code)
	         ruleRecord.ruleConditions = retrieveRuleConditions(connection: Connection, ruleId : Long)
	         
	         println(ruleRecord.toString())
	         rulesList.append(ruleRecord) 
	       }
	  }
	  
	  rulesList
	}

	def retrieveRuleConditions (connection : Connection, in_ruleId : Long) : ArrayBuffer[ruleConditionRecord] = {

	  var ruleConditions = new ArrayBuffer[ruleConditionRecord]()
	  var stmt1 = connection.createStatement
	  	  	  
	  var queryRuleDetails =  "SELECT id,rule_id,open_bracket,s_column_id,s_formula,s_tolerance_type,s_many, " +
                                   "t_column_id,t_formula,t_many,t_tolerance_type,close_bracket," +
                                   "logical_operator,s_column_field_name,s_tolerance_operator_from," + 
                                   "s_tolerance_value_from,s_tolerance_operator_to, s_tolerance_value_to," +
                                   "t_tolerance_operator_from,t_tolerance_value_from ,t_tolerance_operator_to," +
                                   "t_tolerance_value_to,s_value_type , s_operator , s_value,t_value_type ,t_operator," +
                                   "t_value, value_type, operator, value" + 
                            " FROM " +  DBObj.dbName + ".t_rule_conditions WHERE rule_id = " + in_ruleId
	  
	   println ("query : " + queryRuleDetails )     
	       
	   val rs5 = stmt1.executeQuery(queryRuleDetails)
	   
	   while (rs5.next()) {
	     
	     var ruleCondRecord = new ruleConditionRecord()
	     ruleCondRecord.id_=(rs5.getLong("id"))
	     
	     if (rs5.getString("open_bracket") != null ) {
	       ruleCondRecord.openBracket_=(rs5.getString("open_bracket"))
	     }
	    
	     if (rs5.getLong("s_column_id") != null ) {
	       ruleCondRecord.sColumnId_=(rs5.getLong("s_column_id"))
	     }
	     
	     if (rs5.getString("s_formula") != null ) {
	       ruleCondRecord.sFormula_=(rs5.getString("s_formula"))
	     }
	      
	     if (rs5.getString("s_tolerance_type") != null ) {
	       ruleCondRecord.sToleranceType_=(rs5.getString("s_tolerance_type"))
	     }
	     if (rs5.getBoolean("s_many") != null ) {
	       ruleCondRecord.sMany_=(rs5.getBoolean("s_many"))
	     }
	     if (rs5.getLong("t_column_id") != null ) {
	       ruleCondRecord.tColumnId_=(rs5.getLong("t_column_id"))
	     }
	     if (rs5.getString("t_formula") != null ) {
	       ruleCondRecord.tFormula_=(rs5.getString("t_formula"))
	     }
	     if (rs5.getBoolean("t_many") != null) {
	       ruleCondRecord.tMany_=(rs5.getBoolean("t_many"))
	     }
	     if (rs5.getString("t_tolerance_type") != null) {
	       ruleCondRecord.tToleranceType_=(rs5.getString("t_tolerance_type"))
	     }
	     if (rs5.getString("close_bracket") != null) {
	       ruleCondRecord.closeBracket_=(rs5.getString("close_bracket"))
	     }
	     if (rs5.getString("logical_operator") != null ) {
	       ruleCondRecord.logicalOperator_=(rs5.getString("logical_operator"))
	     }
	     if (rs5.getString("s_column_field_name") != null ) {
	       ruleCondRecord.sColumnFieldName_=(rs5.getString("s_column_field_name"))
	     }
	     if (rs5.getString("s_tolerance_operator_from") != null ) {
	       ruleCondRecord.sToleranceOperatorFrom_=(rs5.getString("s_tolerance_operator_from"))
	     }
	     if (rs5.getString("s_tolerance_value_from") != null) {
	       ruleCondRecord.sToleranceValueFrom_=(rs5.getString("s_tolerance_value_from"))
	     }
	     if (rs5.getString("s_tolerance_operator_to") != null) {
	       ruleCondRecord.sToleranceOperatorTo_=(rs5.getString("s_tolerance_operator_to"))
	     }
	     if (rs5.getString("s_tolerance_value_to") != null ) {
	       ruleCondRecord.sToleranceValueTo_=(rs5.getString("s_tolerance_value_to"))
	     }
	     
	     if (rs5.getString("t_tolerance_operator_from") != null ) {
	       ruleCondRecord.tToleranceOperatorFrom_=(rs5.getString("t_tolerance_operator_from"))
	     }
	     if (rs5.getString("t_tolerance_value_from") != null) {
	       ruleCondRecord.tToleranceValueFrom_=(rs5.getString("t_tolerance_value_from"))
	     }
	     if (rs5.getString("t_tolerance_operator_to") != null) {
	       ruleCondRecord.tToleranceOperatorTo_=(rs5.getString("t_tolerance_operator_to"))
	     }
	     if (rs5.getString("t_tolerance_value_to") != null ) {
	       ruleCondRecord.tToleranceValueTo_=(rs5.getString("t_tolerance_value_to"))
	     }
	     if (rs5.getString("s_value_type") != null ) {
	       ruleCondRecord.sValueType_=(rs5.getString("s_value_type"))
	     }
	     if (rs5.getString("s_operator") != null ) {
	       ruleCondRecord.sOperator_=(rs5.getString("s_operator"))
	     }
	     if (rs5.getString("s_value") != null ) {
	       ruleCondRecord.sValue_=(rs5.getString("s_value"))
	     }
	     
	     if (rs5.getString("t_value_type") != null ) {
	       ruleCondRecord.tValueType_=(rs5.getString("t_value_type"))
	     }
	     if (rs5.getString("t_operator") != null ) {
	       ruleCondRecord.tOperator_=(rs5.getString("t_operator"))
	     }
	     if (rs5.getString("t_value") != null ) {
	       ruleCondRecord.tValue_=(rs5.getString("t_value"))
	     }	     
	     if (rs5.getString("value_type") != null ) {
	       ruleCondRecord.valueType_=(rs5.getString("value_type"))
	     }
	     if (rs5.getString("operator") != null ) {
	       ruleCondRecord.operator_=(rs5.getString("operator"))
	     }
	     if (rs5.getString("value") != null) {
	       ruleCondRecord.value_=(rs5.getString("value"))
	     }
	     
	     println ("s_column_id :" + rs5.getString("s_column_id"))
	     if (rs5.getString("s_column_id") != null) {
	        
	        var stmt2 = connection.createStatement
	        val queryColumn = " SELECT ref_dv_type, ref_dv_name, ref_dv_column, column_name, col_data_type, qualifier " +
                 " FROM " +  DBObj.dbName + ".t_data_views_columns " + " WHERE (id = " + rs5.getLong("s_column_id") + ")" 
	       
          println ("query : " + queryColumn )     
	       
	        val rs6 = stmt2.executeQuery(queryColumn)
//	        println ("rs6 count :" + rs6.getFetchSize())
//	        if (rs6.getFetchSize() > 0) {
	          
	          while (rs6.next()) {
	            ruleCondRecord.sColDataType_=(rs6.getString("col_data_type"))
	            if (rs6.getString("ref_dv_type").equalsIgnoreCase("File Template")) {
	               var stmt3 = connection.createStatement
                 val querycolumnSetDetails = "SELECT master_table_reference_column,date_format,time_format,amount_format,column_alias" +
                                             " FROM " +  DBObj.dbName + ".t_file_template_lines" +
                                             " WHERE (id = " + rs6.getString("ref_dv_column") + ")"
                 println ("query : " + querycolumnSetDetails )     
	       
	               val rs7 = stmt3.executeQuery(querycolumnSetDetails)
	              
//	               if (rs7.getFetchSize > 0) {
	                 while (rs7.next()) {  
	                  ruleCondRecord.sRefDvColumn_=(rs6.getString("ref_dv_column"))
	                  
	                  if (ruleCondRecord.sColDataType != null ) {
	                  
	                    if (ruleCondRecord.sColDataType.equalsIgnoreCase("date")) {
	                       ruleCondRecord.sColDataFormat_=(rs7.getString("date_format"))
	                    } else if (ruleCondRecord.sColDataType.equalsIgnoreCase("time")) {
	                       ruleCondRecord.sColDataFormat_=(rs7.getString("time_format"))
	                    } else if (ruleCondRecord.sColDataType.equalsIgnoreCase("decimal")) {
	                      ruleCondRecord.sColDataFormat_=(rs7.getString("amount_format"))
	                    } 
	                    ruleCondRecord.sColumnName_=(rs7.getString("column_alias")) 
	                  }
	                 }
//	               }
	            } else if (rs6.getString("ref_dv_type").equalsIgnoreCase("Data View")) {
	             
	              ruleCondRecord.sRefDvColumn_=("DERIVED_COLUMN")
	              
	              if (ruleCondRecord.sColDataType != null ) {
	                
	                if (ruleCondRecord.sColDataType.equalsIgnoreCase("date")) {
	                   ruleCondRecord.sColDataFormat_=("yyyy-MM-dd")
	                } else if (ruleCondRecord.sColDataType.equalsIgnoreCase("decimal")) {
	                   ruleCondRecord.sColDataFormat_=("")
	                   ruleCondRecord.sMany_=(true)
	                }
	                ruleCondRecord.sColumnName_=(rs6.getString("column_name"))
	              }
	            }
	          }
//	        }
	     }

	     if (rs5.getString("t_column_id") != null) {
	        
	        var stmt4 = connection.createStatement
	        val queryColumn = " SELECT ref_dv_type, ref_dv_name, ref_dv_column, column_name, col_data_type, qualifier " +
                 " FROM " +  DBObj.dbName + ".t_data_views_columns " + " WHERE (id = " + rs5.getLong("t_column_id") + ")" 
	       
          println ("query : " + queryColumn )     
	       
	        val rs6 = stmt4.executeQuery(queryColumn)
	   
//	        if (rs6.getFetchSize > 0) {
	          
	          while (rs6.next()) {	       
	            ruleCondRecord.tColDataType_=(rs6.getString("col_data_type"))
	            if (rs6.getString("ref_dv_type").equalsIgnoreCase("File Template")) {
	               var stmt5 = connection.createStatement
                 val querycolumnSetDetails = "SELECT master_table_reference_column,date_format,time_format,amount_format,column_alias" +
                                             " FROM " +  DBObj.dbName + ".t_file_template_lines" +
                                             " WHERE (id = " + rs6.getString("ref_dv_column") + ")"
                 println ("query : " + querycolumnSetDetails )     
	       
	               val rs7 = stmt5.executeQuery(querycolumnSetDetails)
	              
//	               if (rs7.getFetchSize > 0) {
	                 while (rs7.next()) {
	                  ruleCondRecord.tRefDvColumn_=(rs6.getString("ref_dv_column"))
	                  
	                  if (ruleCondRecord.tColDataType != null ) {
	                  
	                    if (ruleCondRecord.tColDataType.equalsIgnoreCase("date")) {
	                       ruleCondRecord.tColDataFormat_=(rs7.getString("date_format"))
	                    } else if (ruleCondRecord.tColDataType.equalsIgnoreCase("time")) {
	                       ruleCondRecord.tColDataFormat_=(rs7.getString("time_format"))
	                    } else if (ruleCondRecord.tColDataType.equalsIgnoreCase("decimal")) {
	                      ruleCondRecord.tColDataFormat_=(rs7.getString("amount_format"))
	                    } 
	                    ruleCondRecord.tColumnName_=(rs7.getString("column_alias")) 
	                  }
	                  }
//	               }
	            } else if (rs6.getString("ref_dv_type").equalsIgnoreCase("Data View")) {
	             
	              ruleCondRecord.tRefDvColumn_=("DERIVED_COLUMN")
	              
	              if (ruleCondRecord.tColDataType != null ) {
	                
	                if (ruleCondRecord.tColDataType.equalsIgnoreCase("date")) {
	                   ruleCondRecord.tColDataFormat_=("yyyy-MM-dd")
	                } else if (ruleCondRecord.tColDataType.equalsIgnoreCase("decimal")) {
	                   ruleCondRecord.tColDataFormat_=("")
	                   ruleCondRecord.tMany_=(true)
	                }
	                ruleCondRecord.tColumnName_=(rs6.getString("column_name"))
	              }
	            }
	          }
//	        }
	     }	     
	     println(ruleCondRecord.toString())
	     ruleConditions.append(ruleCondRecord)
	   }
	   ruleConditions 
	}
	
	def closeMysqlConn(connection: Connection) : Unit = {
	  connection.close()
	}
	
}