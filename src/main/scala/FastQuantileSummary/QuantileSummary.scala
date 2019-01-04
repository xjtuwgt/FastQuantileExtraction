package FastQuantileSummary

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

class QuantileSummary(numOfLevels: Int, epsilon: Float = 0.001f) {

  var sketch: Array[(Float, Float, Float)] = _ //Summary of the data
  var numOfDataPoint: Float = _ //Number of points in original data
  var quantileArray: Array[Float] = _ //Quantiles constructed over sketch

  /**
    * @param data
    * @return
    */
  def ConstuctSummary(data: Array[Float]): Array[(Float, Float, Float)] = {
    val time1 = System.currentTimeMillis()
    val levelSummaries = MultiLevelQuantileSummary(data)
    sketch = mergeAllSummary(levelSummaries)
    val time2 = System.currentTimeMillis()
    println("==============Sketch construction completed============")
    println("Sketch size : " + sketch.length + " and the original data size: " + numOfDataPoint.toInt)
    println("Compress ratio : " + (sketch.length / numOfDataPoint * 100).formatted("%.4f") + "(%)")
    println("Construction time : " + (time2 - time1) + "(ms)")
    println("=======================================================")
    sketch
  }

  /**
    * Given a value, this function can estimate its quantile value
    * @param value
    * @return
    */
  def quantileEstimation(value: Float): Float = {
    if (value < sketch(0)._1) {
      val q = (sketch(0)._2 + sketch(0)._3) / (2 * numOfDataPoint)
      return q
    }
    if (value > sketch.last._1) {
      val q = (sketch.last._2 + sketch.last._3) / (2 * numOfDataPoint)
      return q
    }
    var lo = 0
    var hi = sketch.length - 1
    while (lo <= hi) {
      val mid = (hi + lo) / 2
      if (value < sketch(mid)._1) {
        hi = mid - 1
      } else if (value > sketch(mid)._1) {
        lo = mid + 1
      } else {
        val q = (sketch(mid)._2 + sketch(mid)._3) / (2 * numOfDataPoint)
        return q
      }
    }
    val q = (sketch(hi)._2 + sketch(hi)._3) / (2 * numOfDataPoint)
    q
  }

  /**
    * Given a quantile value, the function returns the estimated value
    * @param q
    * @return
    */
  def quantileValueEstimation(q: Float): Float = {
    if (q < 0 || q > 1) {
      return -1.0f
    }
    val numQuantiles = sketch.length
    if (numQuantiles == 0) {
      return -1.0f
    }
    if (quantileArray == null || quantileArray.length == 0) {
      quantileArray = quantileValueSetEstimation(numQuantiles + 1)
    }
    val qIdx = (numQuantiles * q + 0.5).toInt
    quantileArray(qIdx)
  }

  /**
    * Return a number of quantiles (given the number of quantiles)
    * @param numOfQuantiles
    * @return
    */
  def quantileValueSetEstimation(numOfQuantiles: Int): Array[Float] = {
    var N = numOfQuantiles
    if (numOfQuantiles < 2) N = 2
    val output: Array[Float] = new Array[Float](N + 1)
    var curId = 0
    var nextId = 0
    for (rank <- 0 to N by 1) {
      val d2 = 2.0f * (rank * sketch.last._3 / numOfQuantiles)
      nextId = curId + 1
      while (nextId < sketch.length && d2 >= (sketch(nextId)._2 + sketch(nextId)._3)) {
        nextId = nextId + 1
      }
      curId = nextId - 1
      if (nextId == sketch.length || d2 < sketch(curId + 1)._2 + sketch(nextId - 1)._3) {
        output(rank) = sketch(curId)._1
      } else {
        output(rank) = sketch(nextId)._1
      }
    }
    output
  }

  /**
    * @param data
    * @return multi-level sketches of the data
    */
  private def MultiLevelQuantileSummary(data: Array[Float]): Array[Array[(Float, Float, Float)]] = {
    numOfDataPoint = data.length
    val N: Int = data.length
    var b: Int = Math.ceil(Math.log(epsilon * N) / Math.log(2.0) / epsilon).toInt
    if (b <= 0) b = N
    val LMax = Math.floor(Math.log(N * 1.0 / b) / Math.log(2.0)).toInt + 2
    println("==============Sketch constructing======================")
    println("Maximum level number: " + LMax + "\nBatch size: " + b)
    println("Error bound: " + epsilon)
    val L = if (numOfLevels <= LMax) LMax else LMax
    val summaryArray: Array[ArrayBuffer[(Float, Float, Float)]] =
      new Array[ArrayBuffer[(Float, Float, Float)]](L + 1)
    for (i <- 0 to L) {
      summaryArray(i) = new ArrayBuffer[(Float, Float, Float)]()
    }

    val buffer: ArrayBuffer[Float] = new ArrayBuffer[Float]()
    var sketch: Array[(Float, Float, Float)] = Array.empty

    var index: Int = 0
    var bufferNum = 0
    while (index < N) {
      buffer.append(data(index))
      if (buffer.length == b) {
        val sortedData = buffer.sorted.zipWithIndex.map(x => (x._1, x._2 + 1.0f, x._2 + 1.0f))
        sketch = Compress(sortedData.toArray, b)
        buffer.clear()
        breakable {
          for (l <- 1 to L) {
            if (summaryArray(l).length == 0) {
              summaryArray(l).append(sketch: _*)
              break
            } else {
              val temp = Merge(summaryArray(l).toArray, sketch)
              sketch = Compress(temp, b)
              summaryArray(l).clear()
            }
          }
        }
        bufferNum = bufferNum + 1
      }
      index = index + 1
    }

    if (!buffer.isEmpty) {
      summaryArray(0) = buffer.sorted.zipWithIndex.map(x => (x._1, x._2 + 1.0f, x._2 + 1.0f))
    }
    summaryArray.map(_.toArray)
  }

  /**
    * @param data sorted in an ascending order
    * @param b    batch size
    * @return     sketch of the data
    */
  private def Compress(data: Array[(Float, Float, Float)], b: Int): Array[(Float, Float, Float)] = {
    val dataSize = data.length
    var data_range: Float = 0.0f
    var e: Float = 0.0f
    data.foreach(point => {
      if (data_range < point._3) {
        data_range = point._3
      }
      if ((point._3 - point._2) > e) {
        e = point._3 - point._2
      }
    })
    if (e > 2 * epsilon * data_range) {
      println("Wrong wrong wrong")
    }

    val sumBuffer: ArrayBuffer[(Float, Float, Float)] = new ArrayBuffer[(Float, Float, Float)]()
    sumBuffer.append(data(0))
    var i = 1
    var j = 1
    var rank = Math.floor(i * 2.0f * data_range / b).toInt
    while (rank < data_range && j < dataSize) {
      breakable {//Find the index of the first data whose rank is greater than rank
        while (j < dataSize) {
          if (data(j)._3 >= rank) {
            break
          }
          j = j + 1
        }
      }
      if (j < dataSize) {
        sumBuffer.append(data(j))
        j = j + 1
      }
      i = i + 1
      rank = Math.floor(i * 2.0f * data_range / b).toInt
    }
    sumBuffer.append(data.last)
    sumBuffer.toArray
  }

  /**
    * @param sketchK
    * @param sketchC
    * @return
    */
  private def Merge(sketchK: Array[(Float, Float, Float)],
                    sketchC: Array[(Float, Float, Float)]): Array[(Float, Float, Float)] = {
    val lenK = sketchK.length
    val lenC = sketchC.length
    val sketch: Array[(Float, Float, Float)] = new Array[(Float, Float, Float)](lenK + lenC)
    var indK = 0
    var indC = 0
    var ind = 0
    while (indK < lenK && indC < lenC) {
      if (sketchK(indK)._1 < sketchC(indC)._1) {
        val v = sketchK(indK)._1
        var rmin = 0.0f
        if (indC > 0) {
          rmin = sketchK(indK)._2 + sketchC(indC - 1)._2
        } else {
          rmin = sketchK(indK)._2
        }
        val rmax = sketchK(indK)._3 + sketchC(indC)._3 - 1
        sketch(ind) = (v, rmin, rmax)
        indK = indK + 1
      } else {
        val v = sketchC(indC)._1
        var rmin = 0.0f
        if (indK > 0) {
          rmin = sketchC(indC)._2 + sketchK(indK - 1)._2
        } else {
          rmin = sketchC(indC)._2
        }
        val rmax = sketchK(indK)._3 + sketchC(indC)._3 - 1
        sketch(ind) = (v, rmin, rmax)
        indC = indC + 1
      }
      ind = ind + 1
    }

    while (indK < lenK) {
      val v = sketchK(indK)._1
      val rmin = sketchK(indK)._2 + sketchC(lenC - 1)._2
      val rmax = sketchK(indK)._3 + sketchC(lenC - 1)._3
      sketch(ind) = (v, rmin, rmax)
      indK = indK + 1
      ind = ind + 1
    }

    while (indC < lenC) {
      val v = sketchC(indC)._1
      val rmin = sketchC(indC)._2 + sketchK(lenK - 1)._2
      val rmax = sketchC(indC)._3 + sketchK(lenK - 1)._3
      sketch(ind) = (v, rmin, rmax)
      indC = indC + 1
      ind = ind + 1
    }
    sketch
  }

  /**
    * Merge the summary over all different levels
    * @param summaryArray
    * @return
    */
  private def mergeAllSummary(summaryArray: Array[Array[(Float, Float, Float)]]): Array[(Float, Float, Float)] = {
    val trueSummary = summaryArray.filter(_.length > 0)
    var allSummary = trueSummary(0)
    for (i <- 1 to trueSummary.length - 1) {
      allSummary = Merge(allSummary, trueSummary(i))
    }
    allSummary
  }
}