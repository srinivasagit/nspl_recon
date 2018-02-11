package com.recon.reconcile

import java.util.Properties
import com.mysql.jdbc.Driver

object DBObj {
	    var dbHost: String = null
			var dbName: String = null
			var dbUser: String = null
			var dbPass: String = null
			var ruleGroupId: Int = _
			var ruleId: Int = _
			var accRuleGroupId: Int = _
			var accAppRuleId: Int = _
			var ruleType : String = null
			var userId: String = null
			var oozieJobId : Long = _
			var tenantId: Long = _
			var contract_num : Long = _
//			println ("contract Initiavalue : " + contract_num)
			var mySqlUrl : String = null
			val mySqlDriver = "com.mysql.jdbc.Driver"
			var properties : Properties = _
			val module: String = "RECON"

			def utils(arguments : Array [String]):Unit = {
					this.dbHost = arguments(0) 
					this.dbName  = arguments(1)
					this.dbUser = arguments(2)
					this.dbPass = arguments(3)
					this.ruleGroupId = arguments(4).toInt
					this.ruleId = arguments(5).toInt
					this.accRuleGroupId = arguments(6).toInt
					this.accAppRuleId = arguments(7).toInt
					this.ruleType = arguments(8)
					this.userId = arguments(9)
					this.oozieJobId = arguments(10).toLong
					this.mySqlUrl= "jdbc:mysql://" + this.dbHost + '/' + this.dbName
					
					println ("Host     			: %s".format(dbHost))
					println ("Database 			: %s".format(dbName))
					println ("DB User  			: %s".format(dbUser))
					println ("Password 			: %s".format(dbPass))
					println ("Rule GroupId	: %d".format(ruleGroupId))
					println ("Rule Id				: %d".format(ruleId))
					println ("Account Rule GroupId: %d".format(accRuleGroupId))
					println ("Account Rule Id			: %d".format(accAppRuleId))
					println ("Rule Type			: %s".format(ruleType))
					println ("User					: %s".format(userId))
					println ("oozieJobId		: %d".format(oozieJobId))
					println ("MySql url 		: %s".format(mySqlUrl))
      }
	    
      def setTenantId(tenant :Long): Unit = {
		      this.tenantId = tenant
      }
      
      def setContract_num(contract_num :Long): Unit = {
		      this.contract_num = contract_num
      }
      
      def buildProps():Properties = {
       		val properties = new Properties
      		properties.setProperty("driver", mySqlDriver)
      		properties.setProperty("user", dbUser)
      		properties.setProperty("password", dbPass)
          properties
      }
}