package Core
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
object Functions {
def trimf(z:Double,p:Seq[Double]):Double={
            var (a,b,c,y)=(p(0),p(1),p(2),0.0)
            if ((z<=a) || (z>=c))           { y = 0.0 }
            else if ((a <= z) && (z<= b))   { y = ((z-a) / (b-a).toDouble)}
            else if ((b <= z)&& (z <= c))   { y = (c-z) / (c-b).toDouble}
            return y
            }

def trimf_tu(z:Double,p:Seq[Double],tu:Double):Double={
            var (a,b,c,y)=(p(0)+tu,p(1)+tu,p(2)+tu,0.0)
            if ((z<=a) || (z>=c))           { y = 0.0 }
            else if ((a <= z) && (z<= b))   { y = ((z-a) / (b-a).toDouble)}
            else if ((b <= z)&& (z <= c))   { y = (c-z) / (c-b).toDouble}
            return y
            }
def getMatchingDegree_RW_udf( ABC : Array[Array[Double]] ) = udf (   (sample: Seq[Double],mfs: Seq[Int],rw:Double) =>  sample.zip(mfs).map(x => (trimf(x._1 , ABC(x._2-1)))).reduceLeft(_ * _)*rw  )         
def getMatchingDegree_udf   ( ABC : Array[Array[Double]] ) = udf (   (sample: Seq[Double],mfs: Seq[Int]) =>  sample.zip(mfs).map(x => (trimf(x._1 , ABC(x._2-1)))).reduceLeft(_ * _)  )

def getTime(duration:Long):String={
  val h = duration / 3600000; val m = (duration % 3600000) / 60000; val s = ((duration % 3600000) % 60000).toFloat / 1000f;
  val str = f"(hh:mm:ss.ms) = $h%02d:$m%02d:$s%2.4f"
  return str}
 def GenerateSubRules(r:Row, parameters:Parameters) :ArrayBuffer[((Seq[Int],Double),(Seq[Int],Double,Double,Double,Double,Double,Double,Double,Double,Double,Seq[Double],Seq[Double]))] = {        
     /* AllrulesInfo= ((Antecedants,FinalClass:Double),Antecedants: Seq[Int], FinalClass:Double, RW: Double, SP: Double, Conf: Double,  num_WR:Double , num_well_classifeid:Double,HasTwoMass: Double,var Sup1_cr:Double,var Sup2_cr:Double, var Prot2: Seq[Double],  Prot: Seq[Double])*/    
      val AllrulesInfo = new ArrayBuffer[((Seq[Int],Double),(Seq[Int],Double,Double,Double,Double,Double,Double,Double,Double,Double,Seq[Double],Seq[Double]))]() 
           
           val allAntecedents=r(0).asInstanceOf[Seq[Int]]
           val allVarsList = for (i <- 0 until allAntecedents.length) yield i
           val allVars = Set(allVarsList: _*)
           var subsetAnts: Array[Int] = null
           var subsetVars: Array[Int] = null
           var subsetsVars: Iterator[Set[Int]] = null
           var len = parameters.minLength; 
           while (len <= parameters.maxLength){
               subsetsVars = allVars.subsets(len)
              // Compute the hash of each subset
               while (subsetsVars.hasNext) {
                      subsetVars = subsetsVars.next().toArray.sorted
                      subsetAnts = Array.fill(allAntecedents.length)(0)
                      var varIdx = 0
                      while (varIdx < subsetVars.length) {
                        
                            subsetAnts(subsetVars(varIdx)) = allAntecedents(subsetVars(varIdx))
                            varIdx += 1
                      }       
//                      var a = ((subsetAnts.toSeq,r(1).asInstanceOf[Double]),  ( subsetAnts.toSeq,r(1).asInstanceOf[Double],r(2).asInstanceOf[Double],r(3).asInstanceOf[Double],r(2).asInstanceOf[Double],r(4).asInstanceOf[Seq[Double]] ) )
/*   Row:r=(0:Antecedants,1:FinalClass,HasTwoMass,SP1,SP2,Prototype1,Prototype2)   */                    
//                   var a = ( (subsetAnts.toSeq,r(1).asInstanceOf[Double]) , new FuzzyRule_Pr_Gen(subsetAnts.toSeq,r(1).asInstanceOf[Double],0.0,0.0,0.0,0.0,0.0,r(2).asInstanceOf[Double],r(3).asInstanceOf[Double],r(4).asInstanceOf[Double],r(5).asInstanceOf[Seq[Double]],r(6).asInstanceOf[Seq[Double]] ))
  
                      
       val matchclass=EvaluateByPrototype(subsetAnts.toSeq,r(5).asInstanceOf[Seq[Double]],parameters,1.0)      // the matching degree of the prototype of those examples that are in class k
       val matchnotclass= EvaluateByPrototype(subsetAnts.toSeq,r(6).asInstanceOf[Seq[Double]],parameters,1.0)   // the matching degree of the prototype of those examples that are not in class k                                     
//       val Supp= (matchclass + matchnotclass)/(r(3).asInstanceOf[Double]+r(4).asInstanceOf[Double])
//       val Conf= matchclass /(matchclass + matchnotclass)                                      
//       val RW=(matchclass - matchnotclass)/(matchclass + matchnotclass) 
                      
      val Supp= r(3).asInstanceOf[Double]
      val Conf= r(3).asInstanceOf[Double] /(r(3).asInstanceOf[Double] + r(4).asInstanceOf[Double])                                      
      val RW=Conf
                      
                   var a = ( (subsetAnts.toSeq,r(1).asInstanceOf[Double]) , (subsetAnts.toSeq,r(1).asInstanceOf[Double],RW,Supp,Conf,0.0,0.0,r(2).asInstanceOf[Double],r(3).asInstanceOf[Double],r(4).asInstanceOf[Double],r(5).asInstanceOf[Seq[Double]],r(6).asInstanceOf[Seq[Double]] ))
            

                     /* return array of Rules info: ((Antecedants, FinalClass), FuzzyRule_Pr_Gen) */
                      AllrulesInfo.append(a)  
                }
              len += 1
            }
           AllrulesInfo
   } //----------------------------------------- 


def calcConfusionMatrix(df:DataFrame,parameters:Parameters,nClasses:Int,predictionLable:String): Array[Array[Long]]={
val confMat = df.rdd.mapPartitions(examples => {
      // Initialize confusion matrix
      var i, j: Int = 0
      val confMat: HashMap[(Int, Int), Long] = new HashMap[(Int, Int), Long]()
      while (i < nClasses) {
        j = 0
        while (j < nClasses) {
          confMat((i, j)) = 0l
          j += 1
        }
        i += 1
      }
      var example: org.apache.spark.sql.Row = null
      var label: Double = 0.0
      var prediction: Double = 0.0
      while (examples.hasNext){
        example = examples.next()
        label = example.getAs(parameters.classLable)
        prediction = example.getAs(predictionLable)
        confMat((label.toInt, prediction.toInt)) += 1l
      }
      confMat.toIterator
    }).reduceByKey(_ + _).collect()    
    // Transform the confusion matrix into an array
    val confusionMatrix: Array[Array[Long]] = Array.fill(nClasses){Array.fill(nClasses){0}}
    confMat.foreach {
      case ((real, predicted), count) =>
        confusionMatrix(real)(predicted) = count;
    }
    confusionMatrix
  } 

  def computeAccuracyStats(df:DataFrame,parameters:Parameters,predictionLable:String): Array[Double] = {
    val nClasses = 2
    val confusionMatrix=calcConfusionMatrix(df,parameters,nClasses,predictionLable)    
    // Compute stats
    val hits: Array[Long] = Array.fill(nClasses){0}
    val nExamples: Array[Long] = Array.fill(nClasses){0}
    val TPrates: Array[Double] = Array.fill(nClasses){0}
    var gm: Double = 1
    var avgacc: Double = 0
    var i: Int = 0
    while (i < nClasses) {
      hits(i) = confusionMatrix(i)(i)
      nExamples(i) = confusionMatrix(i).sum
      TPrates(i) = hits(i).toDouble / nExamples(i).toDouble
      gm *= TPrates(i)
      avgacc += TPrates(i)
      i += 1
    }
    
    val confusionMatrixStr = confusionMatrix.map( _.mkString("\t") ).mkString("\n")
    println("\n ** confusionMatrixStr =   \n" + confusionMatrixStr)
    
    avgacc /= nClasses.toDouble
    gm = math.pow(gm, 1f / nClasses).toDouble
    val acc: Double = hits.sum.toDouble / nExamples.sum.toDouble
    
        
//    println("\n**Acc = " + acc + ", GM = " + gm + ", Avg_Acc = " + avgacc+" **")
    Array(roundAt(5)(acc), roundAt(5)(gm), roundAt(5)(avgacc))
  }

def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

    
def EvaluateByPrototype(Antecedants:Seq[Int],sample: Seq[Double],parameters:Parameters,RW:Double):  Double ={  
   val ABC=parameters.ABC;   var MD: Double = 1.0
   MD= func(Antecedants,sample,0,ABC,MD)
   if (parameters.useRW ==true)    return MD*RW   else   return MD
  } 
   
 def func(Antecedants:Seq[Int],sample: Seq[Double],index:Int,ABC:Array[Array[Double]],MD:Double):  Double ={  
     if (MD > 0  & index < sample.length)  
     { if (Antecedants(index)!=0)
       func(Antecedants,sample,index+1,ABC, MD*trimf(sample(index),ABC(Antecedants(index)-1)))
       else
       func(Antecedants,sample,index+1,ABC, MD*1)  
     }
     else MD
   } 

}    

//      /** Compute confusion matrix **/
//    val nClasses = 2
//    val confusionMatrix: Array[Array[Long]] = Array.fill(nClasses){Array.fill(nClasses){0}}
//
//    val trueRate: Array[Long] = Array.fill(nClasses){0}
//
//    var total, correct: Long = 0
//    countByValue.foreach{
//      case ( (real, predicted), count ) =>
//        confusionMatrix(real)(predicted) = count; total += count; if (real == predicted){ correct += count; trueRate(real) += count}
//    }
//    val confusionMatrixStr = confusionMatrix.map( _.mkString("\t") ).mkString("\n")
//    println(confusionMatrixStr)
//
//    val gm: Double = Math.pow(trueRate.zip(confusionMatrix.map(x => x.sum)).map(y => y._1.toDouble / y._2.toDouble).product, 1/nClasses.toDouble)
//
//    println("Geometric mean: " + gm)
//
//
//    /** Accuracy **/
//    correct.toFloat / total.toFloat








