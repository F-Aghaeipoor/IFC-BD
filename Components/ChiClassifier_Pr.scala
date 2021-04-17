
package Components
import Core.Parameters
import Core.Functions.{getMatchingDegree_RW_udf,getMatchingDegree_udf,trimf,EvaluateByPrototype,roundAt,GenerateSubRules,computeAccuracyStats}

import org.apache.spark.ml.{Estimator,Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//object RuleBase {
//  var rb1:Broadcast[RulesBase]=_
//  var rrrr:DataFrame=_
//  def getRB():RulesBase={ return rb1.value} 
//
//}//-----------------------------------------------------------------------------------------------------

class ChiClassifier_Pr (override val uid: String,parameters:Parameters) extends Estimator[ChiModel_Pr]{
    var rb=new RulesBase()
    override def copy(extra: ParamMap): ChiClassifier_Pr = {defaultCopy(extra)}
    override def transformSchema(schema: StructType): StructType = schema
            
    def GenerateCHiRules(df1:DataFrame):DataFrame={    
            parameters.posLabel=0.0; parameters.negLabel=1.0      
            val numPositiveLables=df1.filter(df1(parameters.classLable)===parameters.posLabel).count() ;  val numNegetiveLables=parameters.numOfSamples - numPositiveLables
            parameters.numOfPositiveSamples=numPositiveLables;  parameters.numOfNegitiveSamples=numNegetiveLables;
            if (numPositiveLables > numNegetiveLables) {parameters.majority_Class = parameters.posLabel} else if(numPositiveLables < numNegetiveLables) {parameters.majority_Class = parameters.negLabel} else { val r = new scala.util.Random  ;    val r1 =  r.nextInt(( 1 - 0) + 1) ;  parameters.majority_Class =r1}         
           
             val inputCol="labels"
             val newNames = Seq("Antecedants", "Class","features")       
             var df2=  df1.select(inputCol,parameters.classLable,"features_arr").toDF(newNames: _*)
              
             val dfP=df2.filter(df2("Class") === parameters.posLabel).groupBy("Antecedants").agg(count("Antecedants").alias("countP"),array((0 until parameters.features.length) map (i => avg(col("features")(i))): _*).alias("avgOfPosSample"))  // 1= positive class
             val dfN=df2.filter(df2("Class") === parameters.negLabel).groupBy("Antecedants").agg(count("Antecedants").alias("countN"),array((0 until parameters.features.length) map (i => avg(col("features")(i))): _*).alias("avgOfNegSample"))  // 0= negetive class                           
            /* dfIntersect : a dataframe including intersect antecedants and number of ecah class lable
               dfIAntecedants : a dataframe including just intersect antecedants
               ruleset1 : coflicted rules(modified), ruleset2 : non-coflicted positive rules, ruleset3 : non-coflicted negetive rules,*/               
            val   dfIntersect = dfP.join(dfN, dfP("Antecedants") === dfN("Antecedants")).drop(dfP("Antecedants"))       
            val   dfIAntecedants = dfIntersect.select(col("Antecedants").alias("iAntecedants"))  
            var   ruleset1 = dfIntersect.withColumn("FinalClass",when(col("countP") > col("countN") , parameters.posLabel).when(col("countP") < col("countN"), parameters.negLabel).when(col("countP") === col("countN"),parameters.majority_Class))
                  ruleset1 = ruleset1.withColumn("HasTwoMass",lit(1.0.toDouble))
                  ruleset1 = ruleset1.withColumn("SP1",when(col("countP") > col("countN") , col("countP").cast("Double")).when(col("countP") < col("countN"), col("countN").cast("Double")).when(col("countP") === col("countN"),col("countN").cast("Double")))
                  ruleset1 = ruleset1.withColumn("SP2",when(col("countP") > col("countN") , col("countN").cast("Double")).when(col("countP") < col("countN"), col("countP").cast("Double")).when(col("countP") === col("countN"),col("countN").cast("Double")))
                  ruleset1 = ruleset1.withColumn("Prototype1",when(col("countP") > col("countN") , col("avgOfPosSample")).when(col("countP") < col("countN"), col("avgOfNegSample")).when(col("countP") === col("countN"),col(if (parameters.majority_Class == parameters.posLabel) "avgOfPosSample" else "avgOfNegSample")))
                  ruleset1 = ruleset1.withColumn("Prototype2",when(col("countP") > col("countN") , col("avgOfNegSample")).when(col("countP") < col("countN"), col("avgOfPosSample")).when(col("countP") === col("countN"),col(if (parameters.majority_Class == parameters.posLabel) "avgOfNegSample" else "avgOfPosSample"))).drop("countP","countN","avgOfPosSample","avgOfNegSample")
                                   
            var ruleset2 = dfP.join(dfIAntecedants, dfP("Antecedants") === dfIAntecedants("iAntecedants"),"left").filter(col("iAntecedants").isNull).withColumn("FinalClass",lit(parameters.posLabel)).withColumn("HasTwoMass",lit(0.0.toDouble)).withColumn("SP1",col("countP").cast("Double")).withColumn("SP2",lit(0.0.toDouble)).withColumn("Prototype1",col("avgOfPosSample")).withColumn("Prototype2",col("avgOfPosSample")).drop("countP","iAntecedants","avgOfPosSample")
            var ruleset3 = dfN.join(dfIAntecedants, dfN("Antecedants") === dfIAntecedants("iAntecedants"),"left").filter(col("iAntecedants").isNull).withColumn("FinalClass",lit(parameters.negLabel)).withColumn("HasTwoMass",lit(0.0.toDouble)).withColumn("SP1",col("countN").cast("Double")).withColumn("SP2",lit(0.0.toDouble)).withColumn("Prototype1",col("avgOfNegSample")).withColumn("Prototype2",col("avgOfNegSample")).drop("countN","iAntecedants","avgOfNegSample")            
            val dfs = Seq(ruleset1, ruleset2, ruleset3) 
            var finalRuleset =dfs.reduce(_ union _) 
            
            parameters.numOfOriginalCHiRules=finalRuleset.count()
            if (parameters.useFiltering == true) finalRuleset=filter1_Low_Suppport_Rules(finalRuleset)      
            parameters.numOfFilteredCHiRules=finalRuleset.count()
            return finalRuleset //includs (Antecedants,FinalClass,HasTwoMass,SP1,SP2,Prototype1,Prototype2)            
      }//------------------------------------
     
    def filter1_Low_Suppport_Rules(RB:DataFrame):(DataFrame)={  
           if(parameters.filtering_THS == -1) parameters.filtering_THS=((parameters.numOfSamples-1)/RB.count()).floor.toInt
           return RB.filter(RB("SP1") > parameters.filtering_THS)
       } //------------------------------------

    def GenerateARMRules(CHi_RB:DataFrame,df:DataFrame):RDD[FuzzyRule_Pr]={
        /* CHi_RB:(Antecedants,FinalClass,HasTwoMass,SP1,SP2,Prototype1,Prototype2)  
         * array of Rules info ((Antecedants, FinalClass), Antecedants, FinalClass, RW , SP , Conf, Prot) */           
        /* -------------------------- 1. generate all ARM rules instead of each chi rule--------------------------*/
      val all_ARM_Rules_RDD=CHi_RB.rdd
          /* flatmap putput = ((Antecedants,FinalClass),1=Antecedants, 2=FinalClass, 3=RW, 4=SP, 5=Conf,  6=num_WR, 7=num_well_classifeid,8=HasTwoMass,9=Sup1_cr, 10=Sup2_cr,  11=Prot1,  12=Prot2)*/    
               .flatMap(f=>GenerateSubRules(f,parameters)) // generate all items for a certain rule
    //         .reduceByKey((k,v) => (k._1 ,k._2,k._3, k._4 +v._4,k._5, k._6.zip(v._6).map(a=> (a._1+a._2)/2.0) )  )                 //reduce items of each class i.e., aggregate the common-Ant rules and add their SP, there may remains some conflict between diffrent class  // fake prot = avg of avg
    //         .reduceByKey((k,v) => (k.Antecedants ,k.FinalClass,k.RW, k.Sup1_cr +v.Sup1_cr,k.Conf, k.Prot.zip(v.Prot).map(a=> (a._1*k.Sup1_cr+a._2*v.Sup1_cr)/(k.Sup1_cr+v.Sup1_cr)) )  )  //reduce items of each class i.e., aggregate the common-Ant rules and add their SP, there may remains some conflict between diffrent class  //real prot = real avg                                 
    //         .sortBy(f=>f._2._4).reduceByKey((k,v) => (v._1 ,v._2,v._3, k._4+v._4,v._5, v._6 ))//reduce items of each class i.e., aggregate the common-Ant rules and add their SP, there may remains some conflict between diffrent class  //max sp prot 
                  
 .reduceByKey(  (k,v) => {             val realAvgProt1= k._11.zip(v._11).map(a=> (a._1*k._9+a._2*v._9)/(k._9+v._9))
                                       val realAvgProt2= k._12.zip(v._12).map(a=> (a._1*k._10+a._2*v._10)/(k._10+v._10))
                                       
                                       val Sup1_cr=k._9+v._9
                                       val Sup2_cr=k._10+v._10
                                       
//                                       val matchclass=EvaluateByPrototype(k._1,realAvgProt1,parameters,1.0)      // the matching degree of the prototype of those examples that are in class k
//                                       val matchnotclass= EvaluateByPrototype(k._1,realAvgProt2,parameters,1.0)   // the matching degree of the prototype of those examples that are not in class k
//                                      
//                                       val Supp= (matchclass + matchnotclass)/(Sup1_cr+Sup2_cr)
//                                       val Conf= matchclass /(matchclass + matchnotclass)                                      
//                                       val RW=(matchclass - matchnotclass)/(matchclass + matchnotclass)
                                       
                                       val Supp= k._4+v._4
                                       val Conf= Sup1_cr /(Sup1_cr + Sup2_cr)                                      
                                       val RW=Conf
                                       
                                       (k._1,k._2,RW,Supp,Conf,0.0,0.0,0.0,Sup1_cr,Sup2_cr,realAvgProt1,realAvgProt2)
                           }).cache() 
                                         
          
             /*modify conflict*/
             .map(f=>(f._1._1, f._2)) //map into new schema to modify conflict (reduce by ant) and select the class with max SP, conf of each win calss is also calculated             
//             .reduceByKey((k,v) => if (k._4 > v._4)(k._1 ,k._2,k._3,k._4,k._4/(k._4 + v._4),k._6) else (v._1,v._2,v._3,v._4,v._4/(k._4 + v._4),v._6)).cache()  //.sortBy(f=>f._2._2).sortBy(f=>f._2._4) // sort by class then by support
 
 
 .reduceByKey(  (k,v) => {             val matchclass_K=EvaluateByPrototype(k._1,k._11,parameters,1.0)      // the matching degree of the prototype of those examples that are in class k
                                       val matchnotclass_K=EvaluateByPrototype(k._1,v._11,parameters,1.0)   // the matching degree of the prototype of those examples that are not in class k
                                       val matchclass_V=EvaluateByPrototype(v._1,v._11,parameters,1.0)
                                       val matchnotclass_V=EvaluateByPrototype(v._1,k._11,parameters,1.0)
                                       
//                                       val Supp_K= (matchclass_K + matchnotclass_K)/(k._9+v._9)
//                                       val Conf_K= matchclass_K /(matchclass_K + matchnotclass_K)                                      
//                                       val Supp_V= (matchclass_V + matchnotclass_V)/(k._9+v._9)
//                                       val Conf_V= matchclass_V /(matchclass_V + matchnotclass_V)
                                       
                                       val RW_Fuzzy_K=(matchclass_K - matchnotclass_K)/(matchclass_K + matchnotclass_K)
                                       val RW_Fuzzy_V=(matchclass_V - matchnotclass_V)/(matchclass_V + matchnotclass_V)                                                                            
                                       
//                                       val RW_K=k._3
                                       val RW_K=RW_Fuzzy_K

                                       val Supp_K= k._4
                                       val Conf_K= k._9/(k._9+v._9)                                     
//                                       val RW_V=v._3
                                       val RW_V=RW_Fuzzy_V
                                       val Supp_V= v._4
                                       val Conf_V= v._9/(k._9+v._9)   
                                       
                           if      (RW_Fuzzy_K > RW_Fuzzy_V) (k._1 ,k._2,RW_K,Supp_K,Conf_K,k._6,k._7,k._8,k._9,k._10,k._11,k._12)  //by sup and conf fuzzy
                           else if (RW_Fuzzy_K < RW_Fuzzy_V) (v._1 ,v._2,RW_V,Supp_V,Conf_V,v._6,v._7,v._8,v._9,v._10,v._11,v._12)
                           else {if (k._2== parameters.majority_Class) (k._1 ,k._2,RW_K,Supp_K,Conf_K,k._6,k._7,k._8,k._9,k._10,k._11,k._12)  else (v._1 ,v._2,RW_V,Supp_V,Conf_V,v._6,v._7,v._8,v._9,v._10,v._11,v._12) }
                                      }
                          ).cache() 
           
             
         var ARM_Rules_RDD=all_ARM_Rules_RDD.map(r => new FuzzyRule_Pr (r._2._1,r._2._2,r._2._3,r._2._4,r._2._5,r._2._6,r._2._7,r._2._12)) //0=numWR
  
         parameters.numOfARM_Rules= all_ARM_Rules_RDD.count()
//         val all_ARM_Rules_array = all_ARM_Rules_RDD.collect()
         
          /* -------------------------- 2. filter ARM rules based on minsupp and minconf -----------------------*/        
         val ARM_Rules_filtered_RDD=  filter2_Low_SuppAndconf(ARM_Rules_RDD).cache();  all_ARM_Rules_RDD.unpersist()         
         parameters.numOfFiltered_ARM_Rules=ARM_Rules_filtered_RDD.count()
         ARM_Rules_filtered_RDD                          
  }//----------------------------------------- 
      
    
    def filter2_Low_SuppAndconf(all_ARM_Rules_RDD:RDD[FuzzyRule_Pr]):RDD[FuzzyRule_Pr]={  
//         var posrule=all_ARM_Rules_RDD.filter(f=>f.FinalClass==0.0)
//         var negrule=all_ARM_Rules_RDD.filter(f=>f.FinalClass==1.0)
//         val neg_minconf=0.6
//         val neg_minsupp=0  //           val neg_minsupp=parameters.numOfNegitiveSamples*0.05
//         negrule=negrule.filter(f=>f.SP>neg_minsupp).filter(f=>f.Conf>neg_minconf)
//         val pos_minconf=0.6
//         val pos_minsupp=0 ;  //          val pos_minsupp=parameters.numOfPositiveSamples*0.05
//         posrule=posrule.filter(f=>f.SP>pos_minsupp).filter(f=>f.Conf>pos_minconf) 

       /*Group by class and remove half less confident rules */
         var posrule=all_ARM_Rules_RDD.filter(f=>f.FinalClass==0.0).sortBy(f=>f.Conf) ; 
         var negrule=all_ARM_Rules_RDD.filter(f=>f.FinalClass==1.0).sortBy(f=>f.Conf) ;    
 
         var posrule1=posrule.zipWithIndex()
         val psize=posrule1.count()/2
         posrule = posrule1.filter(_._2 >=psize ).keys
         
         var negrule1=negrule.zipWithIndex()
         val nsize=negrule1.count()/2
         negrule = negrule1.filter(_._2 >= nsize).keys
          
      
         /*Group by class and lenght and remove half less confident rules */
//         var posrule = all_ARM_Rules_RDD.filter(f=>f.FinalClass==0.0)
//         posrule = CostomFiltring(posrule)
//         var negrule = all_ARM_Rules_RDD.filter(f=>f.FinalClass==1.0)
//         negrule = CostomFiltring(negrule)

         var all= posrule.union(negrule)    
         all
   } //----------------------------------------- 
    def CostomFiltring(Rules:RDD[FuzzyRule_Pr]):RDD[FuzzyRule_Pr]={
      var i=1
      val prop=Array(0.2,0.3,0.5)
      var out : RDD[FuzzyRule_Pr] = null
      var Toprules : RDD[FuzzyRule_Pr] = null
      var rules_i : RDD[(FuzzyRule_Pr,Long)]=null
      var rules_i_size:Long =0
      var NR:Long = 0
      while (i <= parameters.maxLength)
      {
        
        Toprules = Rules.filter(r=> (r.Antecedants.length-r.Antecedants.count(_== 0)) == i)       
        if (Toprules.count()!=0)
          {
          rules_i=Toprules.sortBy(r=>r.Conf,false).zipWithIndex()
          
//          NR= Math.floor(4*prop(i-1)*parameters.numOfLablesPerVariable*parameters.numOfFeatures).toLong
//          Toprules=rules_i.filter(_._2 <= NR).map(r=> r._1) 
         
          NR=Math.floor(rules_i.count()/2).toLong
          Toprules=rules_i.filter(_._2 <= NR).map(r=> r._1) 
           
          if (out == null ) out=Toprules  else out=out.union(Toprules)
           }
 
      i+=1
      }
      out   
      
    }
    
    
    
    def prunning_lost_Rules(all_ARM_Rules_filtered_RDD:RDD[FuzzyRule_Pr],all_ARM_Rules_filtered_array:Array[FuzzyRule_Pr], df:DataFrame): Array[FuzzyRule_Pr] = {      
         /*compute matching degree of each sample with all rules and find the winning rule for that sample i.e., each sample is mapped to its winning rule */
          parameters.t5= System.currentTimeMillis()
          val matching_array=generateMatching(all_ARM_Rules_filtered_array,df)  
          parameters.t6= System.currentTimeMillis()
         
          /*count number of win for each rule(RW) and update rules with new RW feild*/
          val updated_rules_info = Calc_RW(all_ARM_Rules_filtered_RDD,matching_array) 
          parameters.t7= System.currentTimeMillis()
          
          /*Update rules by the new information*/
          val updated_rules_RDD= UpdatRules(updated_rules_info).cache() 
          parameters.rules_test=updated_rules_RDD.collect()

          parameters.t8= System.currentTimeMillis() 
          
          /*remove those rules which were win for less than a minimum samples e.g., */
                 

          var selected_rules= RuleSelection1(updated_rules_RDD, df).cache()
//          var selected_rules= RuleSelection2(updated_rules_RDD, df).cache()
          parameters.numOfPruned_ARM_Rules0=selected_rules.count()
          val out=selected_rules.collect() 
          parameters.numOfPruned_ARM_Rules=out.length
          out
    }//-----------------------------------------
    
    def RuleSelection1(rules_RDD:RDD[FuzzyRule_Pr], df:DataFrame):RDD[FuzzyRule_Pr]={
           var start_tsh=parameters.FS_rate

          var posrule=rules_RDD.filter(f=>f.FinalClass==0.0).cache() 
          var posrule2=posrule.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > start_tsh)
          if (posrule2.count()>0)
            posrule=posrule2
          else
            {var ths=start_tsh-0.1
            posrule2=posrule.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > ths)
            while(posrule2.count()==0)
            {ths=ths-0.1
              posrule2=posrule2.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > ths)}
           posrule=posrule2
            }
           
          var negrule=rules_RDD.filter(f=>f.FinalClass==1.0).cache() 
          var negrule2=negrule.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > start_tsh)
          if (negrule2.count()>0)
            negrule=negrule2
          else 
//           negrule=negrule.filter(r=> r.num_well_classifeid > 0)
            {var ths=start_tsh-0.1
            negrule2=negrule.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > ths)
            while(negrule2.count()==0)
            {ths=ths-0.1
              negrule2=negrule2.filter(r=> r.num_well_classifeid/r.num_WR.toDouble > ths)}
           negrule=negrule2
            }
           
          var out= posrule.union(negrule)
         posrule.unpersist()
         negrule.unpersist()
          out
    }
   def RuleSelection2(rules_RDD:RDD[FuzzyRule_Pr], df:DataFrame):RDD[FuzzyRule_Pr]={
     val pos_rules = rules_RDD.filter(f=>f.FinalClass==0.0).cache() 
     val neg_rules = rules_RDD.filter(f=>f.FinalClass==1.0).cache() 
     
     var maxValue_pos= pos_rules.map(r=> r.num_well_classifeid/r.SP.toDouble).max
     var maxValue_neg= neg_rules.map(r=> r.num_well_classifeid/r.SP.toDouble).max
     
     var rate=parameters.FS_rate
     
     var pos_tsh=maxValue_pos-rate*maxValue_pos
     var neg_tsh=maxValue_neg-rate*maxValue_neg
     var stop=false

     var old_measure=0.0
     var old_selectedRules:RDD[FuzzyRule_Pr]=null
     var out:RDD[FuzzyRule_Pr]=null
     var selected_pos_rules_old:RDD[FuzzyRule_Pr]=null

     var selected_pos_rules= pos_rules.filter(r=> r.num_well_classifeid/r.SP.toDouble > pos_tsh)
     var selected_neg_rules= neg_rules.filter(r=> r.num_well_classifeid/r.SP.toDouble > neg_tsh)
     
     var new_selectedRules=selected_pos_rules.union(selected_neg_rules)     
     var new_measure=evaluateRB(new_selectedRules, df)
    
     var i=1
     while(stop==false )
       
     {  old_selectedRules=new_selectedRules
        old_measure=new_measure
        
        pos_tsh=pos_tsh-rate*maxValue_pos
        neg_tsh=neg_tsh-rate*maxValue_neg
//     
        if (pos_tsh + rate*maxValue_pos>0) 
            selected_pos_rules= pos_rules.filter(r=> r.num_well_classifeid/r.SP.toDouble > pos_tsh)
        else 
           selected_pos_rules=selected_pos_rules_old
          
        if (neg_tsh + rate*maxValue_neg>0) 
          selected_neg_rules= neg_rules.filter(r=> r.num_well_classifeid/r.SP.toDouble > neg_tsh)
        else 
          selected_neg_rules=selected_neg_rules

        new_selectedRules=selected_pos_rules.union(selected_neg_rules)
        new_measure=evaluateRB(new_selectedRules, df)
//      
       
       if (old_measure>new_measure ) {stop=true;  out=old_selectedRules}
       
       if (pos_tsh<0 && neg_tsh<0)   
         if (old_measure>new_measure )
         {stop=true;  out=old_selectedRules}
         else 
         {stop=true;  out=new_selectedRules}
     i+=1
     }
    parameters.iter=i; 
    pos_rules.unpersist()
    neg_rules.unpersist()
     out
   }
   
   
   def infrence_udf3(rb:RulesBase)  = udf (   (sample: Seq[Double]) =>  {  rb.infrence_WinningRule_Pr(sample,parameters) } ) 

   def evaluateRB(RB:RDD[FuzzyRule_Pr], df:DataFrame):Double={
         var df1=df.select(col("features_arr"),col(parameters.classLable)).cache()   
         val rb= new RulesBase(); rb.all_Rules_Pr=RB.collect()  
         var newDF=df1.withColumn("temprary_prediction", infrence_udf3(rb)(col("features_arr"))).cache()
         var Result_train = computeAccuracyStats(newDF,parameters,"temprary_prediction") 
         df1.unpersist()
         newDF.unpersist()
         var accuracy=Result_train(0); var GM= Result_train(1); var avg_accuracy=Result_train(2);
         GM
   }
   
   def infrence_udf2(rb:RulesBase)  = udf (   (sample: Seq[Double], lable:Double) =>  {  rb.infrence_WinningRule_Pr_for_pruning(sample,lable,parameters) } ) 
  
   def generateMatching(all_ARM_Rules_filtered_array:Array[FuzzyRule_Pr], df:DataFrame): Array[(Seq[Int],Long,Double)] ={
         var df1=df.select(col("features_arr"),col(parameters.classLable)).cache()   
         val rb= new RulesBase(); rb.all_Rules_Pr=all_ARM_Rules_filtered_array  
         var matching_df=df1.withColumn("matching_info", infrence_udf2(rb)(col("features_arr"),col(parameters.classLable)) ).select("features_arr","matching_info.*").cache()
         matching_df=matching_df.groupBy(col("_1")).agg(count("_1").alias("num_WR"), sum("_2").alias("num_well_Classified"))
//         parameters.test_DF=matching_df
         val matching_array=matching_df.rdd.map(f=>(f(0).asInstanceOf[Seq[Int]],f(1).asInstanceOf[Long],f(2).asInstanceOf[Double])).collect()  
         df1.unpersist()
         matching_df.unpersist()
         matching_array
   }
   
 
   def Calc_RW(all_ARM_Rules_filtered_RDD:RDD[FuzzyRule_Pr],matching_array:Array[(Seq[Int],Long,Double)]):RDD[(FuzzyRule_Pr,Int,Int)]={
     // val updated_rules_info=all_ARM_Rules_filtered_RDD.map( r=> (r,matching_array.filter(m=> (m._1==r.Antecedants && m._2==r.FinalClass)).length) ).cache()  //by matching rdd
        val updated_rules_info=all_ARM_Rules_filtered_RDD.map(r=> {
        val a=matching_array.filter( m=> m._1==r.Antecedants) ; if (a.length != 0) (r,a(0)._2.toInt,a(0)._3.toInt) else (r,0,0)}  
                                                              )   
        updated_rules_info
   } 
   def UpdatRules(updated_rules_info:RDD[(FuzzyRule_Pr,Int,Int)]):RDD[FuzzyRule_Pr]={
     val updated_rules=updated_rules_info.map(r=>new FuzzyRule_Pr(r._1.Antecedants,r._1.FinalClass,r._1.RW,r._1.SP,r._1.Conf,r._2,r._3,r._1.Prot)).cache()   //RW= number of samples that this rules is win for it  
     updated_rules_info.unpersist()
     updated_rules.filter(r=>r.num_WR>0)
   }

   
     override def fit(df: Dataset[_]): ChiModel_Pr = { 
     parameters.rb= rb;    
     val start = System.currentTimeMillis()
     parameters.t1 = System.currentTimeMillis()
     
     val CHi_Rule_df = GenerateCHiRules(df.toDF())
     parameters.test_DF=CHi_Rule_df
     parameters.t2 = System.currentTimeMillis()
     
     val ARM_Rules_RDD = GenerateARMRules(CHi_Rule_df,df.toDF())
     parameters.t3 = System.currentTimeMillis() 
     
     val Rules_Array=prunning_lost_Rules(ARM_Rules_RDD,ARM_Rules_RDD.collect(),df.toDF());     
     parameters.t4= System.currentTimeMillis() 

     rb.all_Rules_Pr = Rules_Array 
     val stop = System.currentTimeMillis();    val duration = (stop - start) 
      
     
//     rb.all_Rules_Pr_test=CHi_Rule_df.rdd.map(r => new FuzzyRule_Pr (r(0).asInstanceOf[Seq[Int]],r(1).asInstanceOf[Double],r(2).asInstanceOf[Double],r(3).asInstanceOf[Double],r(2).asInstanceOf[Double],r(4).asInstanceOf[Seq[Double]])).collect();
//     rb.all_Rules_Pr_test2=ARM_Rules_RDD.collect()
      
    rb.learning_time = duration.toInt;   rb.numOfRules=rb.all_Rules_Pr.length
//      val rbBroadcast = parameters.sc.broadcast(rb);    // parameters.rb= rbBroadcast;
     parameters.rb= rb;  
    
    val LengthPerRule= parameters.rb.all_Rules_Pr.map(r=> r.Antecedants.length-r.Antecedants.count(_ == 0))
    val TRL=LengthPerRule.reduce(_+_)
    parameters.avg_RL= roundAt(2)(TRL/LengthPerRule.length.toDouble )
    
     val chiModel=new ChiModel_Pr(uid,rb,parameters)    
     return chiModel
      }
           
      
}//End class ChiClassifier--------------------------------------------------------------------------------------


class ChiModel_Pr(override val uid: String,rb: RulesBase,parameters:Parameters)extends Model[ChiModel_Pr] { 
      
     override def copy(extra: ParamMap): ChiModel_Pr = defaultCopy(extra)
     override def transformSchema(schema: StructType): StructType = schema    
     override def transform(df: Dataset[_]): DataFrame = {
     var df1=df.select(parameters.classLable,"features_arr","labels").cache()   
     
     parameters.predictionLable="prediction"
     if (parameters.infrenceMethod == "class_aggrigation")
     {def infrence_udf  = udf (   (sample: Seq[Double]) =>  rb.infrence_classAggrigation_Pr(sample,parameters) )    
     df1=df1.withColumn("prediction",infrence_udf(col("features_arr")))} 
     else   //By default the FMR is Winning Rule 
     {def infrence_udf  = udf (   (sample: Seq[Double]) =>  rb.infrence_WinningRule_Pr(sample,parameters) )
     df1=df1.withColumn("prediction",infrence_udf(col("features_arr")))}  
     return df1
    }
  }//End class ChiModel----------------------------------------------------------------



