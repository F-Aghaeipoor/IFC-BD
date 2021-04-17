package Components
import Core.Parameters
import Core.Functions.{trimf,trimf_tu}

import org.apache.spark.ml.{Estimator,Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame,Dataset}
//import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
//import Components.RuleBase
import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class PrototypeTuning (override val uid: String,parameters:Parameters) extends Estimator[PrototypeTuningModel] {  
override def copy(extra: ParamMap): PrototypeTuning = {defaultCopy(extra)}
      override def transformSchema(schema: StructType): StructType = schema     
      override def fit(df: Dataset[_]): PrototypeTuningModel = {       
      val prototypeTuningModel=new PrototypeTuningModel(uid,parameters.rb,parameters)  
      return prototypeTuningModel}
      
    }//End class MFsTuning--------------------------------------------------------------------------------------

           
      
class PrototypeTuningModel (override val uid: String,rb: RulesBase,parameters:Parameters)extends Model[PrototypeTuningModel]  { 
      
     override def copy(extra: ParamMap): PrototypeTuningModel = defaultCopy(extra)
     override def transformSchema(schema: StructType): StructType = schema
    
     override def transform(df: Dataset[_]): DataFrame = {
     var df1=df.select(parameters.classLable,"features_arr").cache()   
     
     parameters.predictionLable="prediction_tu"
     if (parameters.infrenceMethod == "class_aggrigation")   
     {def infrence_udf  = udf (   (sample: Seq[Double]) =>  rb.infrence_classAggrigation_tu(sample,parameters) )     
     df1=df1.withColumn("prediction_tu",infrence_udf(col("features_arr")))} 
     else   //By default the FMR is Winning Rule  
     {def infrence_udf  = udf (   (sample: Seq[Double]) =>  rb.infrence_WinningRule_tu(sample,parameters) )
     df1=df1.withColumn("prediction_tu",infrence_udf(col("features_arr"))) }  
     
     return df1  
    }
  }//End class MFsTuningModel----------------------------------------------------------------
//

  
//      def findProtoType(df1:DataFrame):(DataFrame,DataFrame)={  
//        var df2 = df1.groupBy("labels",parameters.classLable).agg(count(parameters.classLable).alias("SP"),array((0 until parameters.features.length) map (i => avg(col("features_arr")(i))): _*).alias("avgOfAllSamplesPerCell")).orderBy("SP")
//        var df3=df2.groupBy("labels").agg(max("SP").alias("maxSP"),last("avgOfAllSamplesPerCell").alias("Prototypes"))
////        df3=df3.orderBy("labels")
//        return (df3,df3)
//      }
    