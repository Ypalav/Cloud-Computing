import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd._

object SparkTeraSort{
        def main(args: Array[String]) {
              val sparkConf = new SparkConf().setMaster("spark://ip-172-31-0-173:7077").setAppName("SparkTeraSort")
              val sc = new SparkContext(sparkConf)
               /*creating an inputRDD to read text file (in.txt) through Spark context*/
              val inputFile = sc.textFile("hdfs://ec2-52-17-38-124.eu-west-1.compute.amazonaws.com:9000/input_dir/10GBFile")
              /* Transform the inputRDD into dataRDD by splitting values by newline, then add key and values, then sort by key and merge in 1 file and save it as a text file*/
              val data = inputFile.flatMap(line => line.split("\n")).map(line => (line.substring(0,10), line.substring(11))).sortByKey().coalesce(1).map(x=>x._1+"  "+x._2).saveAsTextFile("hdfs://ec2-52-17-38-124.eu-west-1.compute.amazonaws.com:9000/output10GB/")
              sc.stop()
        }
}
          
