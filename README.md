# Spark ETL Framework

## Idea
We use [stack machine](https://en.wikipedia.org/wiki/Stack_machine) and 
postfix ([Reverse Polish notation](https://en.wikipedia.org/wiki/Reverse_Polish_notation)) to implement an ETL Framework.

Application has a config file that defines tasks of your application. 
The config file is a [hocon](https://en.wikipedia.org/wiki/HOCON) file.


## Build
```bash
sbt clean compile assembly
sbt 'set test in assembly := {}' clean assembly
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
```
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
* repartition

### Spark Operators

Operator has a *name* to determine kind of operator.

#### input
*input* operator is operand. So *input* operator doesn't pop anything from stack.
```
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
```
spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("etl/src/test/resources/data/input.csv")
```

#### output
*output* is unary operator. So *output* operator pop one dataframe from stack that denote by *df*
```
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
```
df.write
    .mode("overwrite")
    .format("csv")
    .repartition(1)
    .partitionBy("act_id", "act_name")
    .option("delimiter", ",")
    .save("etl/src/test/resources/output_data/output.csv")
```
#### join
*join* is binary operator. So it pop two dataframe from stack.
```
# dfl
{
  name = "input"
  path = "etl/src/test/resources/data/films.csv"
  format = "csv"
  options = [
    { key = "delimiter" , value = "," }
    { key = "header", value = "true" }
  ]
}
# dfr
{
  name = "input"
  path = "etl/src/test/resources/data/input.csv"
  format = "csv"
  options = [
    { key = "delimiter" , value = "," }
    { key = "header", value = "true" }
  ]
}
{
  name = "join"
  join-type = "inner"
  conditions = "dfl.act_id = dfr.id"
  select = ["dfr.id as act_id, dfr.name as act_name, dfr.age as age, dfl.name as film"]
}
```
#### union
*union* is nary operator. So you have to define number of operands that pop from stack.
```hocon
{
  name = "union"
  number-of-input = 3
}
```
#### dedup
*dedup* is unary operator.
```hocon
{ 
  name = "dedup" 
  cols = ["id", "age"] 
}
```
Equivalent:
```
df.dropDuplicates(["id", "age"])
```
#### drop
*drop* is unary operator.
```hocon
{ 
  name = "drop" 
  cols = ["id"] 
}
```
Equivalent:
```
df.drop("id")
```
#### rename
*rename* is unary operator
```hocon
{ 
  name = "rename"
  renamed { id = "I_D", name = "N_AME" } 
}
```
Equivalent:
```
df.withColumnRenamed("id", "I_D")
  .withColumnRenamed("name", "N_AME") 
```
#### alias
*alias* is unary operator and it returns without dataframe. In other words, it returns ***None***
```hocon
{ 
  name = "alias"
  alias-name = "input" 
}
```
#### load-alias
*load-alias* is unary operator
```hocon
{ 
  name = "load-alias"
  alias-name = "input" 
}
```
#### incremental
*incremental* is binary operator
```
{ 
  name = "load-alias"
  alias-name = "new-mapping" 
}
{ 
  name = "load-alias"
  alias-name = "all-mapping" 
}
{ 
  name = "select"
  select = ["max(CID) as max"] 
}
{ 
  name = "incremental"
  cols = ["CID"] 
}
```
With above config, application loads *all-mapping*, *new-mapping* dataframes and create *CID* column in *new-mapping* dataframe. *CID* value is incremented start *max(CID)* of *all-mapping* dataframe.
#### except
*except* is binary operator
```hocon
{ 
  name = "load-alias"
  alias-name = "new-mapping" 
}
{ 
  name = "load-alias"
  alias-name = "all-mapping"
}
{ 
  name =  "except" 
}
```
With above config, application loads *new-mapping*, *all-mapping* and except all records that included in *all-mapping* from *new-mapping*
#### view
*view* is unary operator and it returns without a dataframe.
```hocon
{ 
  name = "view"
  view-name = "df" 
}
```
Equivalent:
```
df.createOrReplaceTempView("df") 
```
#### sql
*sql* is operand.
```hocon
{ 
  name = "sql"
  query = "select * from df" 
}
```
### repartition
*repartition* is unary operator
```hocon
{
  name = "repartition"
  partitions = 1
}
```
Equivalent:
```
df.repartition(1) 
```

### save-as-table 
*save-as-table* is unary operator
```hocon
{ 
  name = "save-as-table"
  path = "output_hive"
  mode = "append"
  partitions = 1 
}
```
Equivalent:
```
df.write
    .mode("append")
    .repartition(1)
    .saveAsTable("output_hive")
```

## Intercept udf functions
You need create a class that extended from **SparkUdfInterceptor** trait and implement **intercept(spark: SparkSession)** function. 

Example:  
```scala
class StringUdf extends SparkUdfInterceptor with Logging{

  override def intercept(spark: SparkSession): Unit = {
    println("StringUdf intercept")
    lazy val stringUdfFuncs = Map(
      "uppercase" -> udf((x: String) => x.toUpperCase)
    )

    stringUdfFuncs.foreach {
      case (name, func) => spark.udf.register(name, func)
    }
  }
}

```
