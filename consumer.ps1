$SPARK_SUBMIT = "docker compose exec spark-master /opt/spark/bin/spark-submit"
$JAVA_OPTS_DRIVER = '--conf "spark.driver.extraJavaOptions=-Divy.home=/tmp -Duser.home=/tmp"'
$JAVA_OPTS_EXECUTOR = '--conf "spark.executor.extraJavaOptions=-Divy.home=/tmp -Duser.home=/tmp"'
$PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

Invoke-Expression "$SPARK_SUBMIT $JAVA_OPTS_DRIVER $JAVA_OPTS_EXECUTOR --master spark://spark-master:7077 --packages $PACKAGES /opt/spark/src/sparkConsumer.py"
Start-Process powershell -ArgumentList "-Command", "docker compose exec spark-master ..."