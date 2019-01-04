package Examples

import scala.util.Random
import FastQuantileSummary.QuantileSummary

object FQSTestExample {

  def main(args: Array[String]): Unit = {
    normalRandomNumber()
  }

  def normalRandomNumber(): Unit ={
    val rand = new Random(2l)
    val array = new Array[Float](5000000)
    for(i <- 0 to array.length - 1){
      array(i) = rand.nextGaussian().toFloat
    }
    val FQS = new QuantileSummary(10, 0.001f)
    FQS.ConstuctSummary(array)
    val quantileArray = FQS.quantileValueSetEstimation(5)
    quantileArray.foreach(x=>{
      println(x)
    })
  }

  def UniformRandomNumber(): Unit ={
    val rand = new Random(2l)
    val array = new Array[Float](10000000)
    for(i <- 0 to array.length - 1){
      array(i) = rand.nextDouble().toFloat
    }
    val FQS = new QuantileSummary(10, 0.001f)
    FQS.ConstuctSummary(array)
    val quantileArray = FQS.quantileValueSetEstimation(5)
    quantileArray.foreach(x=>{
      println(x)
    })
  }
}
