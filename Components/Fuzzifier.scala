package Components
import Core.Parameters

import org.apache.spark.ml.{Transformer}
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vector,DenseVector}
import org.apache.spark.sql.functions._
import breeze.stats._
import scala.collection.mutable.ArrayBuffer

class Fuzzifier(override val uid: String, n:Int, parameters:Parameters) extends Transformer{
 
    var bins = ArrayBuffer(-0.1.toDouble); val numChaumck=((n-2)*2+2); val partitionLenght = 1.0/numChaumck
    val b=for{ x <-1 to (numChaumck) by 2 } yield x*partitionLenght
    bins=bins ++ b+=1.1
    parameters.bins=bins
    parameters.ABC=getMFsParams()
    
    def getMFsParams(): Array[Array[Double]]={     
    val ABC= Array.ofDim[Double](n, 3) 
    val p=(1.0/((n-2)*2+2))*2
    ABC(0)(0)=0-p;  ABC(0)(1)=0;  ABC(0)(2)=0+p;
    for (i <- 0 to (n-2)){var a = ABC(i)(1); var b = ABC(i)(2); ABC(i+1)(0)=a;  ABC(i+1)(1)=b;  ABC(i+1)(2)=b+p;}
    return ABC
 }   
    
 override def transform(df: Dataset[_]): DataFrame = {
     var df1=df
     
     val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
     val toArrUdf = udf(toArr)
     val df2 = df1.withColumn("features_arr",toArrUdf(col("features_vec")))
     
     def getLabels_udf(bins:ArrayBuffer[Double]) = udf (  (f: Seq[Double]) =>  digitize(f.toArray, bins.toArray)  )
     val df3 = df2.withColumn("labels", getLabels_udf(bins)(col("features_arr")))

     return df3  
 }
    
 override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
 override def transformSchema(schema: StructType): StructType = schema
}