package Core
import Components.{RulesBase,FuzzyRule_Pr}
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.SparkContext
class Parameters extends Serializable{
    /*Running parameters*/
    var sc:SparkContext=_
    var datasetName : String = _                       // args(0)  , args(1)
    var fold:Int=1                                     // args(2)
    var numOfLablesPerVariable:Int=3                   // args(4)
    var infrenceMethod: String = "winning-rule"        // args(5)  :  "winning-rule"  or "class_aggrigation" 
    var useRW: Boolean = true                          // args(6)  : true, false
    var useFiltering: Boolean = false                   // args(7)  : true, false
    var filtering_THS :Int= -1                         // args(8)  : -1 or a positive integer (-1 = DRF(IEEEFUZZ-2020 , an integer to set as MinSupp)
    var do_PR_Tuning: Boolean = true                   // args(9)  : true, false    
    var outputPath:String=_
    var useFS: Boolean = false                        // args(10)  : true, false
    var FS_rate: Double =1/4.0;
    
    //DB parameters
    var ABC: Array[Array[Double]]=_        // the a b c parameters of triangular MFs: ABC= Array.ofDim[Double](n, 3) 
    var bins :ArrayBuffer[Double] =_
   
    //Data parameters
    var numOfSamples:Long=_
    var numOfPositiveSamples:Long=_
    var numOfNegitiveSamples:Long=_
    var numOfFeatures:Int=_
    var features:Array[String]=_
    var classLable:String=_ 
    var majority_Class:Double=_  
    var posLabel:Double=_
    var negLabel:Double=_
    var selectedFeatures:Array[String]=_
    
    //RB parameters
    var rb:RulesBase=_
    var rules_test:Array[FuzzyRule_Pr]=_
    var avg_RL:Double=_
    var numOfOriginalCHiRules:Long=0 
    var numOfFilteredCHiRules:Long=0 
    var numOfARM_Rules:Long=0 
    var numOfFiltered_ARM_Rules:Long=0 
    var numOfPruned_ARM_Rules:Long=0 
    var numOfPruned_ARM_Rules0:Long=0 

    
    var minLength:Int=1
    var maxLength:Int=3
    //Results parameters
    var accuracy_train:Double=_
    var accuracy_test:Double=_
    
    var avg_accuracy_train:Double=_
    var avg_accuracy_test:Double=_
    
    var GM_train:Double=_
    var GM_test:Double=_
    
    var Chi_Start_time:Long=_
    var Chi_Stop_time:Long=_
    
    var Tuning_Start_time:Long=_
    var Tuning_Stop_time:Long=_
    
    var FS_Start_time:Long=_
    var FS_Stop_time:Long=_
    
    var predictionLable:String=_ 
    
    var test_DF:DataFrame=_
    
    var iter:Int=0
    var t1:Long=0
    var t2:Long=0
    var t3:Long=0
    var t4:Long=0
    var t5:Long=0
    var t6:Long=0
    var t7:Long=0
    var t8:Long=0
    var t9:Long=0
    var t10:Long=0
    var t11:Long=0
}