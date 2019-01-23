package com.heavy.monitoring

import org.apache.spark.sql.execution.SparkPlan

class QueueNode(val plan: SparkPlan, val ext: String)
