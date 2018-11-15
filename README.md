# Spark ETL Framework

## Idea
We use [stack machine](https://en.wikipedia.org/wiki/Stack_machine) and 
postfix ([Reverse Polish notation](https://en.wikipedia.org/wiki/Reverse_Polish_notation)) to implement an ETL Framework.

Application has a config file that defines tasks of your application. 
The config file is a [hocon](https://en.wikipedia.org/wiki/HOCON) file.


## Build
```bash
sbt clean compile assembly
```



## Example

### Config file
```hocon
etl = {
  operators = [
    {
      name = "input"
      path = "etl/src/test/resources/data/input.csv"
      format = "csv"
      options = [
        { key = "delimiter" , value = "," }
        { key = "header", value = "true" }
      ]
    }
    { name = "alias", alias-name = "input" }
    { name = "load-alias", alias-name = "input" }
    { name = "load-alias", alias-name = "input" }
    { name = "union", number-of-input = 2 }
    { name = "load-alias", alias-name = "input" }
    { name = "except" }
    { name = "view", view-name = "df" }
    { name = "sql", query = "select * from df" }
    {
      name = "output",
      path = "etl/src/test/resources/output_data/output.csv",
      mode = "overwrite",
      partitions = ${partition} # partition variable
    }
  ]
}
```

### Submit
```bash
sudo -u hdfs spark-submit --master local[4] --conf spark.driver.extraJavaOptions="-Dconfig.file=test_spark_mapping.conf -Dpartition=1" --class com.heavy.etl.apps.ETL etl-assembly-0.1.0-SNAPSHOT.jar
```

**Note:**
```bash
--conf spark.driver.extraJavaOptions="-Dconfig.file=test_spark_mapping.conf -Dpartition=1"
```
* **-Dconfig.file**: path of hocon config file
* **-Dpartition**: *partition* is a variable that is defined in hocon file


<!--- sudo -u hdfs spark-submit --master yarn --conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances=20 --conf spark.yarn.executor.memoryOverhead=2g --conf spark.storage.memoryFraction=0.1 --executor-cores 4 --executor-memory 6g --conf spark.driver.extraJavaOptions="-Dconfig.file=test_spark_mapping.conf -Ddate=20181027 -Dyesterday=20181026" --class com.heavy.etl.apps.ETL etl-assembly-0.1.0-SNAPSHOT.jar --->

## Create config file
As mentioned, config file is a hocon file. It defines all tasks of application as operators.
The config file has to has a **etl** element and **etl.operators** where we define operators.
We support operators as bellow:
* input
* output
* join
* union
* dedup
* drop
* rename
* alias
* load-alias
* incremental
* except
* sql
* view

### Operators

Operator has a *name* to determine kind of operator.

#### input
*input* operator is operand. So *input* operator doesn't pop anything from stack.
```hocon
{
  name = "input"
  path = "etl/src/test/resources/data/input.csv"
  format = "csv"
  options = [
    { key = "delimiter" , value = "," }
    { key = "header", value = "true" }
  ]
}
```
Equivalent:
```scala
spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("etl/src/test/resources/data/input.csv")
```

#### output
*output* is unary operator. So *output* operator pop one dataframe from stack that denote by *df*
```hocon
{
  name = "output",
  path = "etl/src/test/resources/output_data/output.csv",
  mode = "overwrite",
  format = "csv",
  partitions = 1,
  partition-by = ["act_id", "act_name"],
  options = [
    { key = "delimiter", value = ","}
  ]
}
```
Equivalent:
```scala
df.write
    .mode("overwrite")
    .format("csv")
    .repartition(1)
    .partitionBy("act_id", "act_name")
    .option("delimiter", ",")
    .save("etl/src/test/resources/output_data/output.csv")
```
#### join
#### union
#### dedup
#### drop
#### rename
#### alias
#### load-alias
#### incremental
#### except
#### sql
#### view
