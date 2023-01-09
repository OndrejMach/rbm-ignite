#!/bin/bash
startdate=2022-06-15
enddate=2022-12-07
d=
n=0
until [ "$d" = "$enddate" ]
do
    ((n++))
    d=$(date -d "$startdate + $n days" +%Y-%m-%d)
    echo $d
    /opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=tc -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=tp -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=cg -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=cr -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=mk -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=mt -date=$d

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client --num-executors 24 --executor-cores 8 --executor-memory 20G --driver-memory 20G --conf spark.dynamicAllocation.enabled=false --driver-java-options "-Dlog4j.configuration=file:/data_ext/apps/sit/rcseu/conf/log4j.custom.properties" --class "com.tmobile.sit.rbm.Processor" /data_ext/apps/sit/rbm/lib/rbm-ignite-1.0-all.jar -natco=st -date=$d

done