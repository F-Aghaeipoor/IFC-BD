package Components
import Core.Functions.{trimf,trimf_tu,roundAt} 
import Core.Parameters
import scala.util.control.Breaks._

class FuzzyRule  (var Antecedants: Seq[Int], var FinalClass:Double, var RW: Double, var SP: Double ) extends Serializable{
  
  /*This method is the best to compute MatchingDegree of each sample with one fuzzy rule*/
   def computeMatchingDegree1(sample: Seq[Double],parameters:Parameters):  Double ={  
   val ABC=parameters.ABC;   var MD: Double = 1.0
   MD= func(sample,0,ABC,MD)
   if (parameters.useRW ==true)    return MD*RW   else   return MD
  } 
   
   def func(sample: Seq[Double],index:Int,ABC:Array[Array[Double]],MD:Double):  Double ={  
     if (MD > 0  & index < sample.length)  
     { if (Antecedants(index)!=0)
       func(sample,index+1,ABC, MD*trimf(sample(index),ABC(Antecedants(index)-1)))
       else
       func(sample,index+1,ABC, MD*1)  
     }
     else MD
   } 
   override def toString(): String = { "( Antecedants = "+ Antecedants.toString()+", FinalClass = " + FinalClass.toString() + ", RW = " + RW.toString() + ", SP =" + SP.toString() + ")"}
}//-----------------------------------------------------------------------------------------------------------------------------------

class FuzzyRule_Pr  ( Antecedants: Seq[Int], FinalClass:Double, RW: Double, SP: Double, var Conf: Double, var num_WR:Double ,var num_well_classifeid:Double, var Prot: Seq[Double]) extends FuzzyRule( Antecedants, FinalClass, RW, SP) with Serializable{ 
   /*This method is the best to compute MatchingDegree of each sample with one fuzzy rule in the tuning mode*/
   def computeMatchingDegree1_tu(sample: Seq[Double],parameters:Parameters):  Double ={  
   var MD: Double = 1.0 ; val ABC=parameters.ABC; 
   MD= func_tu(sample,0,ABC,MD)
   if (parameters.useRW ==true)    return MD*RW   else   return MD
  } 
   
   def func_tu(sample: Seq[Double],index:Int,ABC:Array[Array[Double]],MD:Double):  Double ={  
     if (MD > 0  & index < sample.length)  
     { if (Antecedants(index)!=0)
       func_tu( sample,index+1, ABC,MD*trimf_tu(sample(index),ABC(Antecedants(index)-1), Prot(index)-ABC(Antecedants(index)-1)(1)  ) ) // last exp is the extent of shifting : tu in trimf_tu
       else 
       func_tu( sample,index+1, ABC,MD*1 )
     }

     else MD
   }
   
   def get():Seq[(Int,Int)]={
     var out= Antecedants.zipWithIndex.filter(a => a._1 != 0 )
     out
   }
 //  override def toString(): String = { "( Antecedants = "+ Antecedants.toString()+", FinalClass = " + FinalClass.toString() + ", RW = " + roundAt(2)(RW).toString() + ", SP = " + roundAt(2)(SP).toString() +", Conf = " +roundAt(2)(Conf).toString() +", num_WR = "+ roundAt(2)(num_WR).toString()+", Prototype = "+Prot+" )"}
 override def toString(): String = { "(Ant_len= "+ (Antecedants.length-Antecedants.count(_ == 0)).toString()+", Ant_info(label, var) = "+get().toString()+", Class = " + FinalClass.toString() + ", RW = " + roundAt(2)(RW).toString() + ", SUP = " + roundAt(0)(SP).toString() +", Conf = " +roundAt(2)(Conf).toString() +", #WR = "+ roundAt(2)(num_WR).toString()+", #well_classifeid = "+ roundAt(2)(num_well_classifeid).toString()+ ", well/WR = "+roundAt(2)(num_well_classifeid/num_WR.toDouble).toString()+" )"}
  
}//-----------------------------------------------------------------------------------------------------------------------------------
   
//class FuzzyRule_Pr_Gen  ( Antecedants: Seq[Int], FinalClass:Double, RW: Double, SP: Double, Conf: Double,  num_WR:Double , num_well_classifeid:Double,var HasTwoMass: Double,var Sup1_cr:Double,var Sup2_cr:Double, var Prot1: Seq[Double],  Prot2: Seq[Double]) extends FuzzyRule_Pr( Antecedants, FinalClass, RW, SP,Conf,num_WR,num_well_classifeid,Prot) with Serializable{ 
//   /*This method is the best to compute MatchingDegree of each sample with one fuzzy rule in the tuning mode*/
// }
   
   
   
   



   /*
   /*This method is time consuming (the worst one)*/
  def computeMatchingDegree3(sample: Seq[Double],parameters:Parameters):  Double ={  
    var MD: Double = 1.0;    val ABC=parameters.ABC
    if (parameters.useRW ==true)  MD = sample.zip(Antecedants).map(x => (trimf(x._1 , ABC(x._2-1)))).reduceLeft(_ * _)*RW
    else   MD = sample.zip(Antecedants).map(x => (trimf(x._1 , ABC(x._2-1)))).reduceLeft(_ * _)   
    return MD
  }
  
  /*This method is time consuming */
  def computeMatchingDegree2(sample: Seq[Double],parameters:Parameters):  Double ={  
   val ABC=parameters.ABC;   var MD: Double = 1.0
   breakable { for( i<-0 to sample.length-1 )
                     { if (MD > 0)  {MD= MD*trimf(sample(i),ABC(Antecedants(i)-1))}  else { MD=0.0;  break} }}
   if (parameters.useRW ==true)    return MD*RW   else   return MD
  }
  */
 
   
  






