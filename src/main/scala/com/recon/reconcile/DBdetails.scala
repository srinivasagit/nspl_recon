package com.recon.reconcile

import java.util.Properties
import com.mysql.jdbc.Driver

class DBdetails {
	   private var _dbHost: String = null
		 private var _dbName: String = null
		 private var _dbUser: String = null
		 private var _dbPass: String = null
		 private var _ruleGroupId: Int = _
		 private var _ruleId: Int = _
		 private var _accRuleGroupId: Int = _
		 private var _accAppRuleId: Int = _
		 private var _ruleType : String = null
		 private var _userId: String = null
	   private var _oozieJobId : Long = _
		 private var _tenantId: Long = _
		 private var _contract_num : Long = _
//			println ("contract Initiavalue : " + contract_num)
		 private var _mySqlUrl : String = null
		 private val _mySqlDriver = "com.mysql.jdbc.Driver"
		 private var properties : Properties = _
		 private val _module: String = "RECON"
//		 private val _target_dbName : String = "guestrecon"

		 def dbHost = _dbHost
		 def dbName = _dbName
		 def dbUser = _dbUser
		 def dbPass = _dbPass
		 def ruleGroupId =_ruleGroupId
		 def ruleId = _ruleId
		 def accRuleGroupId = _accRuleGroupId
		 def accAppRuleId = _accAppRuleId
		 def ruleType = _ruleType
		 def userId = _userId
		 def oozieJobId = _oozieJobId
		 def mySqlUrl = _mySqlUrl
		 def mySqlDriver = _mySqlDriver
		 def module = _module
//		 def target_dbName = _target_dbName
		 def target_dbName = _dbName
		 def target_mySqlUrl= "jdbc:mysql://" + dbHost + '/' + target_dbName
		 
		 def utils(arguments : Array [String]):Unit = {
	     
					this._dbHost = arguments(0) 
					this._dbName  = arguments(1)
					this._dbUser = arguments(2)
					this._dbPass = arguments(3)
					this._ruleGroupId = arguments(4).toInt
					this._ruleId = arguments(5).toInt
					this._accRuleGroupId = arguments(6).toInt
					this._accAppRuleId = arguments(7).toInt
					this._ruleType = arguments(8)
					this._userId = arguments(9)
					this._oozieJobId = arguments(10).toLong
					this._mySqlUrl= "jdbc:mysql://" + this.dbHost + '/' + this.dbName
					
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
		      this._tenantId = tenant
      }
      
      def tenantId = _tenantId
      
      def setContract_num(contract_num :Long): Unit = {
		      this._contract_num = contract_num
      }
      
      def contract_num = _contract_num

      def buildProps():Properties = {
       		val properties = new Properties
      		properties.setProperty("driver", mySqlDriver)
      		properties.setProperty("user", dbUser)
      		properties.setProperty("password", dbPass)
          properties
      }
}