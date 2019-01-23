# Monitoring
We use service loader to register listeners to spark session.

## Monitor module

### QueryExecutionListener
We support **QueryExecutionListener**. Please implement **org.apache.spark.sql.util.QueryExecutionListener** trait and add it to _resources/META-INF/services_ folder.

We have a class that implement **org.apache.spark.sql.util.QueryExecutionListener** in **monitoring** module. It is **com.heavy.monitoring.QueryExecutionListener**

### QueryExecutionSource

**QueryExecutionSource** extends **Source** trait of Spark. Metrics are pushed from **QueryExecutionListener** when **onSuccess** event of **QueryExecutionListener** is triggered.  