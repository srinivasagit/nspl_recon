#/bin/sh
process_time=`date +%m%d%Y_%H%M%S`
log_file="/home/hduser/Desktop/logs/spark_nspl_recon_job_${process_time}.log"
echo $log_file
/usr/local/spark/bin/spark-submit --class com.recon.reconcile.dynamicReconcile \
--total-executor-cores 4 \
/home/hduser/workspace/recon/target/scala-2.11/nspl_recon_2.11-1.0.jar \
localhost:3306 recon_test root 123456 7 2 3 4 i 99999999 10000 > /home/hduser/Desktop/logs/spark_nspl_recon_job_${process_time}.log
