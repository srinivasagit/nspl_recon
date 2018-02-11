package com.recon.reconcile

import  org.apache.spark.sql.SparkSession

class sparkService {
  
  def getSparkSession() : SparkSession = {
         val spark: SparkSession = SparkSession.builder
                                               .appName("NSPL_RECON")
//                                             .master(args(0))
                                               .master("local[*]")
                                               .getOrCreate()
         
          return spark
  }
}