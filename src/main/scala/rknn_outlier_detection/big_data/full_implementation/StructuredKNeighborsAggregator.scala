//package rknn_outlier_detection.big_data.full_implementation
//
//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
//import org.apache.spark.sql.types.{StructField, StructType}
//
//class StructuredKNeighborsAggregator extends UserDefinedAggregateFunction {
//    def inputSchema: org.apache.spark.sql.types.StructType =
//        StructType(StructField("value", BooleanType) :: Nil)
//    def bufferSchema: StructType = StructType(
//        StructField("result", BooleanType) :: Nil
//    )
//    def dataType: DataType = BooleanType
//    def deterministic: Boolean = true
//    def initialize(buffer: MutableAggregationBuffer): Unit = {
//        buffer(0) = true
//    }
//    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//        buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
//    }
//    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//        buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
//    }
//    def evaluate(buffer: Row): Any = {
//        buffer(0)
//    }
//}
