$SPARK_SUBMIT = "docker compose exec spark-master /opt/spark/bin/spark-submit"
$JAVA_OPTS_DRIVER = '--conf "spark.driver.extraJavaOptions=-Divy.home=/tmp -Duser.home=/tmp"'
$JAVA_OPTS_EXECUTOR = '--conf "spark.executor.extraJavaOptions=-Divy.home=/tmp -Duser.home=/tmp"'
$PACKAGES = "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0"

Invoke-Expression "$SPARK_SUBMIT $JAVA_OPTS_DRIVER $JAVA_OPTS_EXECUTOR --master local[*] --packages $PACKAGES /opt/spark/src/sparkBatch.py"