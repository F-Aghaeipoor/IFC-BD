package Core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame}
object InputOutput {
  
   
  def loadLocalData():(SparkSession,DataFrame,DataFrame,Parameters)= {
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
    
    /*val training = spark.createDataFrame(Seq(
        (0.0, "3", 1.0,"N", 1.0),
        (0.0, "3", 1.0,"Y", 1.0),
        (0.0, "3", 1.0,"N", 1.0),
        (0.0, "3", 1.0,"N", 1.0),
        (0.0, "3", 1.0,"Y", 1.0),
        (0.1, "3", 1.0,"N", 1.0),
        (1.0, "4", 0.0,"N", 1.0),
        (1.0, "4", 0.0,"Y", 1.0),
        (1.0, "4", 0.0,"Y", 1.0),
        (1.0, "4", 0.0,"Y", 1.0),
        (2.0, "5", 1.0,"Y", 1.0),
        (1.0, "3", 1.0,"N", 1.0),
        (0.1, "3",1.0,"N", 1.0),
        (3.0, "5", 0.0,"N", 1.0),
        (3.0, "5", 0.8,"N", 1.0),
        (3.0, "5", 0.9,"N", 1.0),
        (3.0, "5", 0.9,"Y", 1.0),
        (2.0, "3.4", 0.9,"N", 1.0),
        (3.0, "5", 0.9,"N", 1.0),
        (3.0, "5", 0.0,"Y", 1.0))).cache()
        
      val testing = spark.createDataFrame(Seq(
        (0.0, "3", 1.0,"N", 1.0),
        (0.0, "4", 1.0,"Y", 1.0),
        (0.0, "3", 1.0,"N", 1.0),
        (2.0, "3.4", 0.9,"Y", 1.0),
        (1.0, "3", 1.0,"Y", 1.0),
        (0.1,"3",1.0,"N", 1.0),
        (3.0, "5", 0.0,"N", 1.0),
        (3.0, "5", 0.8,"N", 1.0))).cache()    */     
    
    val training = spark.createDataFrame(Seq(
        (0.0, "3", 1.0,"N"),
        (0.0, "3", 1.0,"Y"),
        (0.0, "3", 1.0,"N"),
        (0.0, "3", 1.0,"N"),
        (0.0, "3", 1.0,"Y"),
        (0.1, "3", 1.0,"N"),
        (1.0, "4", 0.0,"N"),
        (1.0, "4", 0.0,"Y"),
        (1.0, "4", 0.0,"Y"),
        (1.0, "4", 0.0,"Y"),
        (2.0, "5", 1.0,"Y"),
        (1.0, "3", 1.0,"N"),
        (0.1, "3",1.0,"N"),
        (3.0, "5", 0.0,"N"),
        (3.0, "5", 0.8,"N"),
        (3.0, "5", 0.9,"N"),
        (3.0, "5", 0.9,"Y"),
        (2.0, "3.4", 0.9,"N"),
        (3.0, "5", 0.9,"N"),
        (3.0, "5", 0.0,"Y"))).cache()
        
      val testing = spark.createDataFrame(Seq(
        (0.0, "3", 1.0,"N"),
        (0.0, "4", 1.0,"Y"),
        (0.0, "3", 1.0,"N"),
        (2.0, "3.4", 0.9,"Y"),
        (1.0, "3", 1.0,"Y"),
        (0.1,"3",1.0,"N"),
        (3.0, "5", 0.0,"N"),
        (3.0, "5", 0.8,"N"))).cache()         
    
     val parameters=new Parameters();  
    parameters.do_PR_Tuning=false
    parameters.useFiltering=false
     parameters.datasetName="local"   ;   
     parameters.outputPath="D:\\MyDatasets\\"; 
     return (spark,training,testing,parameters)
     }//------------------------------------------------------------------------------------------------------   
    
   def loadClusterData(args: Array[String]):(SparkSession,DataFrame,DataFrame,Parameters)= {         
    val spark = SparkSession.builder.master("spark://atlas-node:7077").appName("Simple Application").getOrCreate()
    val traPath = "hdfs://atlas-node:8020/user/datasets/"+args(0)+"/"+args(1)+"-5-"+args(2)+"tra."+args(3)
    val tstPath = "hdfs://atlas-node:8020/user/datasets/"+args(0)+"/"+args(1)+"-5-"+args(2)+"tst."+args(3)   
    val training = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(traPath).cache()
    val testing =  spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(tstPath).cache()
   
    val parameters=new Parameters(); 
    parameters.datasetName= args(1); 
    parameters.fold=args(2).toInt;
    parameters.numOfLablesPerVariable= args(4).toInt; 
    parameters.infrenceMethod= args(5);  
    parameters.useRW = args(6).toBoolean;   
    parameters.useFiltering=args(7).toBoolean; 
    parameters.filtering_THS=args(8).toInt
    parameters.outputPath="hdfs://atlas-node:8020/user/ffaghaei/Result_new/";
    parameters.do_PR_Tuning=args(9).toBoolean
    parameters.useFS=args(10).toBoolean
    parameters.FS_rate=args(11).toDouble
    return (spark,training,testing,parameters)
    }//------------------------------------------------------------------------------------------------------   
   
    def loadLocalDataset():(SparkSession,DataFrame,DataFrame,Parameters)= {
    val spark = SparkSession.builder.master("local[*]").config("spark.driver.maxResultSize", 0).appName("Simple Application").getOrCreate()  

    val traPath = "D:\\MyDatasets\\clasification\\haberman\\haberman-5dobscv-1tra.dat"
    val tstPath = "D:\\MyDatasets\\clasification\\haberman\\haberman-5dobscv-1tst.dat"
    
//    val traPath = "D:\\MyDatasets\\clasification\\bupa\\bupa-5dobscv-1tra.dat"
//    val tstPath = "D:\\MyDatasets\\clasification\\bupa\\bupa-5dobscv-1tst.dat"
//    
    var training = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(traPath).cache()
    var testing =  spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(tstPath).cache()
    
    val parameters=new Parameters();  
//    parameters.sc=spark.sparkContext
    parameters.datasetName= "haberman";      
    parameters.outputPath="D:\\MyDatasets\\" 
    return (spark,training,testing,parameters)
    }//------------------------------------------------------------------------------------------------------   
 
    
  def Print_DF_Result(parameters:Parameters,spark:SparkSession):DataFrame={
  import spark.implicits._
  var resultOfCurrentFold:DataFrame =null
  if (parameters.useFS==true)
   resultOfCurrentFold = Seq( (parameters.datasetName,parameters.fold, parameters.accuracy_train,parameters.accuracy_test ,parameters.avg_accuracy_train,parameters.avg_accuracy_test,parameters.GM_train,parameters.GM_test, parameters.numOfLablesPerVariable,parameters.useRW,parameters.useFS,parameters.FS_rate,parameters.features.length,parameters.selectedFeatures.length,parameters.useFiltering,parameters.filtering_THS,parameters.numOfOriginalCHiRules,parameters.rb.numOfRules,parameters.avg_RL,parameters.do_PR_Tuning) ).toDF(Seq("Dataset","Fold", "Train Acc", "Test Acc", "Train Avg Acc", "Test Avg Acc", "Train GM", "Test GM","nLables","useRW","useFS","FS_rate","#F Before","#F Aftre","useFilter","THS","#R Before","#R After","avg_RL","do_PR_Tuning"):_*)
  else
   resultOfCurrentFold = Seq( (parameters.datasetName,parameters.fold, parameters.accuracy_train,parameters.accuracy_test ,parameters.avg_accuracy_train,parameters.avg_accuracy_test,parameters.GM_train,parameters.GM_test, parameters.numOfLablesPerVariable,parameters.useRW,parameters.useFiltering,parameters.filtering_THS,parameters.numOfOriginalCHiRules,parameters.rb.numOfRules,parameters.avg_RL,parameters.do_PR_Tuning) ).toDF(Seq("Dataset","Fold", "Train Acc", "Test Acc", "Train Avg Acc", "Test Avg Acc", "Train GM", "Test GM","nLables","useRW","useFilter","THS","#R Before","#R After","avg_RL","do_PR_Tuning"):_*)

  resultOfCurrentFold.count() ;// 
  resultOfCurrentFold.show()
  resultOfCurrentFold
}

def save_DF_Result(df:DataFrame,parameters:Parameters,spark:SparkSession)={
  var all=df
  if (parameters.fold>1){
   val other = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(parameters.outputPath+parameters.infrenceMethod+parameters.datasetName+".csv")
   all = all.union(other)}  
   
  var result = all.coalesce(1).orderBy("fold").dropDuplicates()
  result.write.mode("append").option("header", "true").format("csv").save(parameters.outputPath+parameters.infrenceMethod+parameters.datasetName+".csv")  
//  result.show()
  result.describe().show()
}


def save_DF_Result_tu(df:DataFrame,parameters:Parameters,spark:SparkSession)={
  var all=df
  if (parameters.fold>1) {
  val other = spark.read.format("csv").option("inferSchema", "true") .option("header", "true").load(parameters.outputPath+parameters.infrenceMethod+parameters.datasetName+"_tu.csv")
  all = all.union(other)}  
  var result = all.coalesce(1).orderBy("fold").dropDuplicates()
  result.write.mode("append").option("header", "true").format("csv").save(parameters.outputPath+parameters.infrenceMethod+parameters.datasetName+"_tu.csv")
//  result.show()
  result.describe().show()
}


def PrintResultOther(parameters:Parameters,spark:SparkSession):DataFrame={
  import spark.implicits._
  val resultOfCurrentFold = Seq( (parameters.datasetName,parameters.fold, parameters.accuracy_train,parameters.accuracy_test) ).toDF(Seq("Dataset Name","Fold", "Train Accuracy", "Test Accuracy"):_*)
  resultOfCurrentFold.count() 
  resultOfCurrentFold.show()
  resultOfCurrentFold
} 
  
}