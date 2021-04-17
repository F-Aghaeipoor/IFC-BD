
package Core
import Components.{CategoricalIndexer,Fuzzifier,ChiClassifier_Pr,PrototypeTuning}
import Core.Functions.{getTime,computeAccuracyStats}
import Core.InputOutput.{loadLocalDataset,loadLocalData,loadClusterData,Print_DF_Result,save_DF_Result,save_DF_Result_tu,PrintResultOther}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.ml.{Pipeline,PipelineModel }
import org.apache.spark.ml.feature.{MinMaxScaler,StringIndexer,VectorAssembler, StandardScaler,InfoThSelector,MDLPDiscretizer}
import org.apache.log4j._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.DecisionTreeClassifier

object Start {
  
  def main(args: Array[String]): Unit =  {
  
    println("Application Started...........")
    Logger.getLogger("org").setLevel(Level.ERROR)
  
//-------------------------------------------------------------------------------------------------------  
    
//    val (spark,training,testing,parameters) = loadLocalData()           // Prepare training data manually.   
//    val (spark,training,testing,parameters) = loadClusterData(args)     // Prepare training data from HDFS in csv format.   
    val (spark,training,testing,parameters) = loadLocalDataset()          // Prepare training data from local system in csv format.         
   
    /*Parametr setting*/    
    val features1 = new CategoricalIndexer().getFeatureNames(training); parameters.numOfSamples=training.count() ; parameters.classLable = features1.drop(features1.size -1)(0);   parameters.features = features1.dropRight(1).toArray;   parameters.numOfFeatures= parameters.features.length ;parameters.selectedFeatures=parameters.features  
    //-------------------------------------------------------------------------------------------------------      
    
    val (training2,testing2) = runChi_Pr(training,testing,parameters,spark)   
//  val (training2,testing2) = runOther(training,testing,parameters,spark)  

//   println(parameters.rules_test.deep.mkString("\n"))   // print array of array
   println("***********************************************************************")
//   println(parameters.rb.all_Rules_Pr.deep.mkString("\n"))   // print array of array

//   parameters.test_DF.filter(col("num_well_Classified")===0).show()
//   parameters.test_DF.show()
   println("\n** Number of Train Partitions = "+ training.rdd.getNumPartitions+" **\t** Number of Test  Partitions = "+ testing.rdd.getNumPartitions+" **")
   println("\n** #Samples = "+ parameters.numOfSamples +", #Pos_Samples = "+ parameters.numOfPositiveSamples+", #Neg_Samples = "+ parameters.numOfNegitiveSamples+ " **" )

//   println("\n** Sum of num_WR = "+ parameters.rb.all_Rules_Pr.map(r=>r.num_WR).sum  )
   
  
   println("\n** #OriginalCHiRules = "+ parameters.numOfOriginalCHiRules +"\n** #FilteredCHiRules = "+ parameters.numOfFilteredCHiRules+"\n** #ARM_Rules = "+ parameters.numOfARM_Rules+"\n** #Filtered_ARM_Rules = "+ parameters.numOfFiltered_ARM_Rules+"\n** #Pruned_low_qualified_rules = "+ parameters.numOfPruned_ARM_Rules0+"\n** #Pruned_low_covering_rules = "+ parameters.numOfPruned_ARM_Rules +" **" )
   
   println("\n** # Pos ARM rules = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==0.0).length+", # Neg ARM rules = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==1.0).length+" **" )

   println("\n\n** # Final Pos rules = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==0.0).length+", # Final Neg rules = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==1.0).length+" **" )
   println("\n** # Pos well claissified = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==0.0).map(r=>r.num_well_classifeid).sum+",  # Neg well claissified = "+ parameters.rb.all_Rules_Pr.filter(r=>r.FinalClass==1.0).map(r=>r.num_well_classifeid).sum)
   
   println("\n\n** # max conf = "+ parameters.rb.all_Rules_Pr.map(r=>r.Conf).max+", min conf = "+ parameters.rb.all_Rules_Pr.map(r=>r.Conf).min)
   println("\n** # max supp = "+ parameters.rb.all_Rules_Pr.map(r=>r.SP).max+", min supp = "+ parameters.rb.all_Rules_Pr.map(r=>r.SP).min)
   
   println("\n** # max well, WR, SP (pos) = "+ parameters.rules_test.filter(r=>r.FinalClass==0.0).map(r=>(r.num_well_classifeid,r.num_WR, r.SP,r.num_well_classifeid/r.SP.toDouble)).maxBy(_._3))
   println("\n** # max well, WR, SP (neg)= "+ parameters.rules_test.filter(r=>r.FinalClass==1.0).map(r=>(r.num_well_classifeid,r.num_WR, r.SP,r.num_well_classifeid/r.SP.toDouble)).maxBy(_._3))
   println("\n** # Iter ="+parameters.iter)
   println("\n\n** Rule Learning Time     " + getTime(parameters.rb.learning_time) + " **")
   
   if(parameters.useFS == true)
      { println("\n**(Features = ("+ parameters.features.mkString(", ") + ") : ClassLabl = " + parameters.classLable + ")")  
        println("\n**(Selected Features = ("+ parameters.selectedFeatures.mkString(", ") + ") : ClassLabl = " + parameters.classLable + ")") 
        println("\n** Feature Selection Time " + getTime(parameters.FS_Stop_time - parameters.FS_Start_time)+" **")  } 
        
   println("\n** Total Running Time     " + getTime(parameters.Chi_Stop_time - parameters.Chi_Start_time)+" **") 
   if(parameters.do_PR_Tuning == true)   
        println("\n** Total Tunning Time     " + getTime(parameters.Tuning_Stop_time - parameters.Tuning_Start_time)+" **") 
        
   println("\n\n****************************************************** Program successfully ended ****************************************************")
   
//   showTime(parameters)
  
}//End of main---------------------------------------------------------------------------------------------------------
   
   
 def runChi_Pr(training1:DataFrame,testing1:DataFrame,parameters:Parameters,spark:SparkSession):(DataFrame,DataFrame)={
    parameters.Chi_Start_time = System.currentTimeMillis()
    var training=training1
    var testing=testing1
    var boolFeatures = for{ i <- 0 to (training.columns.length - 1)  if ("BooleanType"  == (training.dtypes(i)_2)) } yield training.dtypes(i)_1  
    
     for (i <- boolFeatures) 
       {training=training.withColumn(i,col(i).cast("integer"))
        testing=testing.withColumn(i,col(i).cast("integer"))
       }
    
    val categoricalIndexer= new CategoricalIndexer().fit(training);  
    val df1=categoricalIndexer.transform(training)
   
   if(parameters.useFS == true)   
      applyFS(df1,parameters)
      
    val vectorAssembler = new VectorAssembler().setInputCols(parameters.selectedFeatures).setOutputCol("features_raw"); val df2=vectorAssembler.transform(df1)  
    val minMaxScaler = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features_vec").fit(df2)  ;   val df3 =minMaxScaler.transform(df2)   
    val fuzzifier = new Fuzzifier("myFuzzifier",parameters.numOfLablesPerVariable,parameters);  val df4 = fuzzifier.transform(df3) 
    val chiModel=new ChiClassifier_Pr("myChiClassifier",parameters).fit(df4); val trained_df=chiModel.transform(df4) ;     //train_result.show()   
    
//    println("NUM Of RulesBeforFiltering = "+parameters.numOfRulesBeforFiltering +" **** NUM Of RulesBeforFiltering = "+parameters.rb.value.numOfRules)
   
    val df10= categoricalIndexer.transform(testing);val df20 = vectorAssembler.transform(df10);val df30 = minMaxScaler.transform(df20);         
    val df40=fuzzifier.transform(df30) ; val tested_df=chiModel.transform(df40);   // tested_df.show()
    
    var Result_train = computeAccuracyStats(trained_df,parameters,parameters.predictionLable); parameters.accuracy_train=Result_train(0); parameters.GM_train= Result_train(1);parameters.avg_accuracy_train=Result_train(2);
    var Result_test = computeAccuracyStats(tested_df,parameters,parameters.predictionLable);parameters.accuracy_test=Result_test(0); parameters.GM_test= Result_test(1);parameters.avg_accuracy_test=Result_test(2);
    
    var resultOfCurrentFold=Print_DF_Result(parameters,spark)
    parameters.Chi_Stop_time = System.currentTimeMillis();
    save_DF_Result(resultOfCurrentFold,parameters,spark)
    
    if (parameters.do_PR_Tuning == false)
       { return (trained_df,tested_df) }
    else
       { val (trained_df2,tested_df2) = runPrototypeTunning(trained_df,tested_df,parameters,spark)
         return (trained_df2,tested_df2) }
    }//------------------------------------------------------------------------------------------------------    
  
  def applyFS(df:DataFrame,parameters:Parameters)={
    parameters.FS_Start_time= System.currentTimeMillis() 
    val vectorAssembler1 = new VectorAssembler().setInputCols(parameters.features).setOutputCol("features_raw"); val df2_0 = vectorAssembler1.transform(df)     
    val discretizer = new MDLPDiscretizer().setMaxBins(10).setMaxByPart(10000).setInputCol("features_raw") .setLabelCol(parameters.classLable) .setOutputCol("features_vec1")  
    val discretizerModel = discretizer.fit(df2_0);  val df3_1 = discretizerModel.transform(df2_0)   
    import scala.math._
    val numToSelect= math.ceil(parameters.numOfFeatures * parameters.FS_rate)
    val selector = new InfoThSelector()	.setSelectCriterion("mrmr")	.setNPartitions(100).setNumTopFeatures(numToSelect.toInt).setFeaturesCol("features_vec1").setLabelCol(parameters.classLable).setOutputCol("features_vec2")
    val selectorModel = selector.fit(df3_1);   val df3_2 = selectorModel.transform(df3_1)
//    df3_2.show(5) 
    var a= selectorModel.selectedFeatures
    var b=for{ i <-0 to a.length-1 } yield parameters.features(a(i))
    parameters.selectedFeatures =b.toArray 
    parameters.FS_Stop_time=System.currentTimeMillis(); 
  }
  
   
    def runPrototypeTunning(training:DataFrame,testing:DataFrame,parameters:Parameters,spark:SparkSession):(DataFrame,DataFrame)={  
    parameters.Tuning_Start_time = System.currentTimeMillis() 
    val prototypeTuningModel=new PrototypeTuning("myMFsTuning",parameters).fit(training); val trained_df=prototypeTuningModel.transform(training) ;    //train_result_tu.show()   
    val tested_df=prototypeTuningModel.transform(testing)
//    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(parameters.classLable).setPredictionCol("prediction_tu").setMetricName("accuracy")   
//    parameters.accuracy_train = evaluator.evaluate(trained_df)    
//    parameters.accuracy_test  = evaluator.evaluate(tested_df)  
    var Result_train = computeAccuracyStats(trained_df,parameters,parameters.predictionLable); parameters.accuracy_train=Result_train(0); parameters.GM_train= Result_train(1);parameters.avg_accuracy_train=Result_train(2);
    var Result_test = computeAccuracyStats(tested_df,parameters,parameters.predictionLable)  ; parameters.accuracy_test =Result_test(0) ; parameters.GM_test= Result_test(1)  ;parameters.avg_accuracy_test=Result_test(2);   
    var resultOfCurrentFold=Print_DF_Result(parameters,spark)
    parameters.Tuning_Stop_time = System.currentTimeMillis(); 
    save_DF_Result_tu(resultOfCurrentFold,parameters,spark)  
    return (trained_df,tested_df)}//------------------------------------------------------------------------------------------------------   
       
    def runOther(training:DataFrame,testing:DataFrame,parameters:Parameters,spark:SparkSession):(DataFrame,DataFrame)={
    parameters.Chi_Start_time = System.currentTimeMillis()
//    parameters.sc=spark.sparkContext
    val categoricalIndexer= new CategoricalIndexer().fit(training);  val df1=categoricalIndexer.transform(training)
    val vectorAssembler = new VectorAssembler().setInputCols(parameters.features).setOutputCol("features_raw"); val df2=vectorAssembler.transform(df1)      
    val minMaxScaler = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features_vec").fit(df2)  ;   val df3 =minMaxScaler.transform(df2)
    val fuzzifier = new Fuzzifier("myFuzzifier",parameters.numOfLablesPerVariable,parameters);  val df4_0 = fuzzifier.transform(df3) 
    val labelIndexer = new StringIndexer().setInputCol(parameters.classLable) .setOutputCol("indexedLabel") .fit(df4_0)
    val df4_1 = labelIndexer.transform(df4_0)
    parameters.classLable="indexedLabel"
    val Model=new DecisionTreeClassifier().setLabelCol(parameters.classLable).setFeaturesCol("features_vec").fit(df4_1); val trained_df=Model.transform(df4_1) ;     //train_result.show()   
    val df10= categoricalIndexer.transform(testing);val df20 = vectorAssembler.transform(df10);val df30 = minMaxScaler.transform(df20);val df40_0=fuzzifier.transform(df30) ;     val df40_1 = labelIndexer.transform(df40_0)
    val tested_df=Model.transform(df40_1); //    test_result.show()
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(parameters.classLable).setPredictionCol("prediction").setMetricName("accuracy")   
    parameters.accuracy_train = evaluator.evaluate(trained_df)    
    parameters.accuracy_test  = evaluator.evaluate(tested_df)  
    var resultOfCurrentFold=PrintResultOther(parameters,spark)
    parameters.Chi_Stop_time = System.currentTimeMillis();
//    saveDF(resultOfCurrentFold,parameters,spark) // should be write    
    return (trained_df,tested_df)}//------------------------------------------------------------------------------------------------------      
    
    
   def showTime(parameters:Parameters){          
   println("\n** Time 1 (CHi learning) =  " + getTime(parameters.t2 - parameters.t1)+" **")  
   println("\n** Time 2 (ARM learning) =  "+getTime(parameters.t3 - parameters.t2)+" **") 

   println("\n** Time 3 (Pruning) =  " + getTime(parameters.t4 - parameters.t3)+" **") 
   
   println("\n\n** Time 4 (pruning.matching) =  " + getTime(parameters.t6 - parameters.t5)+" **")    
   println("\n** Time 5 (pruning.calc_RW) =  " + getTime(parameters.t7 - parameters.t6)+" **")   
   println("\n** Time 6 (pruning.update_rules) =  " + getTime(parameters.t8 - parameters.t7)+" **")
   
   println("\n\n****************************************************** Program successfully ended ****************************************************")
   }
    
    
    
    
}//End of object start

// "class_aggrigation"   "winning-rule"

    /*val categoricalIndexer= new CategoricalIndexer() ;   val features1 = new CategoricalIndexer().getFeatureNames(training);  val classCol = features1.drop(features1.size -1)(0);   val features = features1.dropRight(1).toArray
    val vectorAssembler   = new VectorAssembler().setInputCols(features).setOutputCol("features_raw")
    val minMaxScalerModel = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features_vec")
    val fuzzifier         = new Fuzzifier("myFuzzifier",3)
    val chiModel          = new ChiClassifier("myChiClassifier",classCol,parameters)   
    val pipeline=new Pipeline().setStages(Array(categoricalIndexer,vectorAssembler, minMaxScalerModel,fuzzifier,chiModel))
    val pipelineModel = pipeline.fit(training)
    val train_result  = pipelineModel.transform(training) ;       train_result.show() 
    val test_result   = pipelineModel.transform(testing)  ;     //  test_result.show() 
    println("Pipeline built....................................................................\n") */


//   println(parameters.rb.value.all_Rules_Pr.deep.mkString("\n"))   // print array of array

//    println(parameters.tuneParam.deep.mkString("\n"))   // print array of array
