package Components
import org.apache.spark.ml.{Estimator,Model}
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.ml.feature.{StringIndexer,StringIndexerModel}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import scala.collection.mutable.ArrayBuffer

//-----------------------------------------------------------------------------------------------------------------------------             

class CategoricalIndexer(override val uid: String) extends Estimator[CategoricalIndexerModel] {
      def this() = this(Identifiable.randomUID("myCategoricalIndexer"))
      override def copy(extra: ParamMap): CategoricalIndexer = {defaultCopy(extra)}
      override def transformSchema(schema: StructType): StructType = schema

      override def fit(df: Dataset[_]): CategoricalIndexerModel = {

      var categoricalFeatures = for{ i <- 0 to (df.columns.length - 2)  if ("StringType"  == (df.dtypes(i)_2)) } yield df.dtypes(i)_1      
      var indexers=new ArrayBuffer[StringIndexerModel]() 
      
      for (i<-categoricalFeatures)  
      {val newStringIndexerModel = new StringIndexer().setInputCol(i).setOutputCol(i+"_indexed").setHandleInvalid("skip").fit(df)
      indexers+=newStringIndexerModel}
      
      val lableCol= df.dtypes(df.columns.length - 1)_1  
      val newStringIndexerModel2 = new StringIndexer().setInputCol(lableCol).setOutputCol(lableCol+"_indexed").setHandleInvalid("skip").fit(df)
      indexers+=newStringIndexerModel2
      
      new CategoricalIndexerModel(uid, indexers,categoricalFeatures)
     }

      def getFeatureNames(df:DataFrame): IndexedSeq[String]=   {
      var categoricalFeatures = for{ i <- 0 to (df.columns.length - 2)  if ("StringType"  == (df.dtypes(i)_2)) } yield df.dtypes(i)_1
      var non_categoricalFeatures = for{ i <- 0 to (df.columns.length - 2)  if ("StringType"  != (df.dtypes(i)_2)) } yield df.dtypes(i)_1
      val categoricalFeaturesIndexed = for{ i <- categoricalFeatures } yield i + "_indexed"  
      val lableIndexed= df.dtypes(df.columns.length - 1)._1
      return non_categoricalFeatures ++ categoricalFeaturesIndexed ++ Seq(lableIndexed+"_indexed")
     }   
}//End class CategoricalIndexer
  
//-----------------------------------------------------------------------------------------------------------------------------             

class CategoricalIndexerModel(override val uid: String, indexers: ArrayBuffer[StringIndexerModel],categoricalFeatures:IndexedSeq[String]) extends Model[CategoricalIndexerModel] {
      override def copy(extra: ParamMap): CategoricalIndexerModel = defaultCopy(extra)
      override def transformSchema(schema: StructType): StructType = schema

      override def transform(df: Dataset[_]): DataFrame = {
      var df1= df
      for( i <- indexers )  {df1 = i.transform(df1) }
      val filterdDF = df1.drop(categoricalFeatures.toArray:_*)
      return filterdDF
}
  }//End class CategoricalIndexerModel
 

