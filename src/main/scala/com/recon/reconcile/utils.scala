package com.recon.reconcile

import java.util.Properties
import com.mysql.jdbc.Driver

class utils(val arguments : Array [String]) {
	    val dbHost: String = arguments(0) 
			val dbName: String = arguments(1)
			val dbUser: String = arguments(2)
			val dbPass: String = arguments(3)
			val ruleGroupId: Int = arguments(4).toInt
			val ruleId: Int = arguments(5).toInt
			val accRuleGroupId: Int = arguments(6).toInt
			val accAppRuleId: Int = arguments(7).toInt
			val ruleType : String = arguments(8)
			val userId: String = arguments(9)
			val oozieJobId : Long = arguments(10).toLong

			val mySqlUrl = "jdbc:mysql://" + dbHost + '/' + dbName
	    val mySqlDriver = "com.mysql.jdbc.Driver"
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
  
			
			def buildProps():Properties ={
	      
	      val prop = new Properties
	      prop.setProperty("driver", mySqlDriver)
	      prop.setProperty("user", dbUser)
	      prop.setProperty("password", dbPass)
	      
	      return prop
      
	    }
}