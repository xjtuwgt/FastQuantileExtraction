# FastQuantileExtraction
This project is constructed by Scala with sbt.

1. File "QuantileSummary.scala" 
	1) Extract the summary of data by function: ConstructSummary(data: Array[Float]), output: Array of tuple (v, rmin, rmax) where rmin = minimum rank w.r.t. v, rmax = maximum rank w.r.t. maximum rank
	2) For a given value, estimate its quantile in the data by function: quantileEstimation(value: Float)
	3) For a given quantile, estimate its value in the data
	4) Given the number of quantiles we want to extract, funtion quantileValueSetEstimation(numOfQuantiles: Int) returns all the quantiles.

2. FQSTestExample
	1) Randomly generate a large set of data follows normal distributions and extrat the summary
	2) Randomly generate a large set of data follows uniform distributions and extrat the summary

3. Performance
	1) For normal distribution: given epsilon as 0.001, for 10M points, about 3732(ms) to construct the summary
	2) For 100M points, about 29481(ms)
