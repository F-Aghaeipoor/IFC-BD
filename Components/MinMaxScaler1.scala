
package Components
import org.apache.spark.ml.{Estimator,Model}
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.ml.feature.{StringIndexer,StringIndexerModel}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import scala.collection.mutable.ArrayBuffer

//-----------------------------------------------------------------------------------------------------------------------------             

class MinMaxScaler1(override val uid: String,InputCol:String,OutputCol:String) extends Estimator[MinMaxScaler1Model] {
      override def copy(extra: ParamMap): MinMaxScaler1 = {defaultCopy(extra)}
      override def transformSchema(schema: StructType): StructType = schema

      override def fit(df: Dataset[_]): MinMaxScaler1Model = {

      
      new MinMaxScaler1Model(uid)
     }


}//End class CategoricalIndexer
  
//-----------------------------------------------------------------------------------------------------------------------------             

class MinMaxScaler1Model(override val uid: String) extends Model[MinMaxScaler1Model] {
      override def copy(extra: ParamMap): MinMaxScaler1Model = defaultCopy(extra)
      override def transformSchema(schema: StructType): StructType = schema

      override def transform(df: Dataset[_]): DataFrame = {
      var df1= df.toDF()
      df1
}
  }//End class CategoricalIndexerModel
 

