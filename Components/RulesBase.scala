package Components
import org.apache.spark.sql.{DataFrame,Dataset}
import Core.Parameters
class RulesBase extends Serializable {
  
  var all_Rules:Array[FuzzyRule]=_
  var all_Rules_Pr:Array[FuzzyRule_Pr]=_
  var all_Rules_Pr_test:Array[FuzzyRule_Pr]=_
  var all_Rules_Pr_test2:Array[FuzzyRule_Pr]=_

  
  var learning_time:Int=_
  var numOfRules:Long=_ 
  def getLearning_time():Int={ return learning_time} 
  
  def infrence_WinningRule(sample:Seq[Double],parameters:Parameters):Double={   
  val MDs=all_Rules.map( r => (r.FinalClass, r.computeMatchingDegree1(sample,parameters))).maxBy(_._2)    
  return MDs._1}
 
  def infrence_classAggrigation(sample:Seq[Double],parameters:Parameters):Double={   
  val CDs=all_Rules.map( r => (r.FinalClass, r.computeMatchingDegree1(sample,parameters))).groupBy(_._1).mapValues( _.map(_._2).sum ).maxBy(_._2)
  return CDs._1}
  
//  def CalcAllMFValues(sample:Seq[Double],parameters:Parameters):Array[Array[Double]]={}
  
  def  find_Winer_Rule(all_Rules_Pr:Array[FuzzyRule_Pr],sample:Seq[Double],parameters:Parameters):(Seq[Int],Double,Double)={
    var i = 0
    val nRules = all_Rules_Pr.length
    var maxMatching, currMatching: Double = 0.0
    var Ant:Seq[Int]= null
    var winClass = parameters.majority_Class // Default class = most frequent class
//    var MFvalues=CalcAllMFValues(sample,parameters)
    while (i < nRules){      
          currMatching = all_Rules_Pr(i).computeMatchingDegree1(sample,parameters)
          if (currMatching > maxMatching) {
            maxMatching = currMatching
            winClass = all_Rules_Pr(i).FinalClass
            Ant=all_Rules_Pr(i).Antecedants
          }        
        i += 1
      }
    // Return winning class
    (Ant,winClass, maxMatching)
  }
  
  def infrence_WinningRule_Pr(sample:Seq[Double],parameters:Parameters):Double={   
//  val MDs=all_Rules_Pr.map( r => (r.FinalClass, r.computeMatchingDegree1(sample,parameters) ) ).maxBy(_._2) 
//    return MDs._1}
  val MDs=find_Winer_Rule(all_Rules_Pr,sample,parameters)  
  return MDs._2}
   
  def infrence_WinningRule_Pr_for_pruning(sample:Seq[Double],lable:Double,parameters:Parameters):(Seq[Int],Double)={   
//  val MDs=all_Rules_Pr.map( r => (r.Antecedants, r.FinalClass, r.computeMatchingDegree1(sample,parameters) ) ).maxBy(_._3)
  val MDs=find_Winer_Rule(all_Rules_Pr,sample,parameters)  
  return (MDs._1, if (MDs._2==lable)  1.0 else 0.0)}  //(Ant , if well_classified 1 else 0)
  
  def infrence_classAggrigation_Pr(sample:Seq[Double],parameters:Parameters):Double={   
  val CDs=all_Rules_Pr.map( r => (r.FinalClass, r.computeMatchingDegree1(sample,parameters))).groupBy(_._1).mapValues( _.map(_._2).sum ).maxBy(_._2)
  return CDs._1}
  
  def infrence_WinningRule_tu(sample:Seq[Double],parameters:Parameters):Double={   
  val MDs=all_Rules_Pr.map( r => (r.FinalClass, r.computeMatchingDegree1_tu(sample,parameters))).maxBy(_._2)    
  return MDs._1}
  
  def infrence_classAggrigation_tu(sample:Seq[Double],parameters:Parameters):Double={   
  val CDs=all_Rules_Pr.map( r => (r.FinalClass, r.computeMatchingDegree1_tu(sample,parameters))).groupBy(_._1).mapValues( _.map(_._2).sum ).maxBy(_._2)
  return CDs._1}
  
  
}