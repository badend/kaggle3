package com.piki_ds

import java.text.SimpleDateFormat

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils

import scala.util.Try
import scala.util.hashing.MurmurHash3

/**
 * Created by jihoonkang on 8/18/15.
 */
object SpringLeaf {


  def getSparkContext()= {

    val conf = new SparkConf().setAppName("ㄲㅎ")


    conf.setMaster("local[8]")
    conf.set("master", "local[8]")
    conf.set("spark.app.name", "sl")
    conf.set("spark.executor.instances", "10")
    conf.set("spark.executor.memory", "1500m")
    conf.set("spark.storage.memoryFraction", "0.5")
    val sc = new SparkContext(conf)
    sc
  }

  val datIdx = Set(217, 157, 179, 156, 169, 73, 166, 176, 204, 159, 167, 177, 158, 75, 168, 178, 168, 73)
  val booIdx = Set(10, 196, 228, 238, 229, 9, 225, 12, 11, 43, 231, 8, 235)
  val skipList = Set(200, 212,402, 491,44,838,213, 207)
  val catIdx = Set(352, 5, 202, 340, 838, 216, 221, 1, 1932, 281, 236, 350, 303, 323, 213, 465, 351, 272, 214, 464)

  val catVal = Map(352 -> Set("U","-1","R","O"),
  1 -> Set("H","R","Q"),
  221 -> Set("C6"),
  464 -> Set("-1","I"),
  465 -> Set("Discharge NA","Dismissed","Discharged","-1"),
  202 -> Set("BatchInquiry"),

  323 -> Set("U","F","M","G","P","H","-1","R","S"),
  5 -> Set("B","C","N","S"),
  214 -> Set("HRE-Social Security Number-1397","HRE-Social Security Number-1289","FSI-0005-1","HRE-Home Phone-0621","HRE-Social Security Number-18823",
    "HRE-Social Security Number-2857","HRE-Social Security Number-1747","HRE-Social Security Number-1855",
    "HRE-Social Security Number-15335","HRE-Social Security Number-1373","HRE-Social Security Number-10143","HRE-Home Phone-0779"),
  236->Set("IN","ID","NM","OR","IA","IL","TN","MO","AZ","AK","WA","SD","KY","NJ","TX","MI","MD","NV","NE","MN","KS","OK","CT","OH","AR","FL","WI","CO","MT"
    ,"DC","PA","GA","HI","WY","LA","CA","UT","AL","WV","VA","NC","NY","SC","MS","DE"),
  281 -> Set("U","F","P","H","-1","R","S"),
  303 -> Set("U","F","M","P","H","-1","R","S"),
  216 -> Set("DS"),
  1932 -> Set("BRANCH","RCC","IAPS","MOBILE","CSC"),
  272 -> Set("MA","IN","ID","NM","OR","IA","IL","TN","PR","MO","ME","AZ","AK","VT","WA","SD","KY","NJ","TX","MI","MD","NV","NE","MN","EE","KS","OK","CT","OH","RR","AR","GS","FL","WI","RN","CO","MT","DC","ND","PA","GA","NH","HI","WY","-1","LA","CA","UT","AL","WV","VA","NC","NY","SC","RI","MS","DE"),
  350 -> Set("U","-1","R","O"),
  340 -> Set("BU","BD","CE","AC","DA","UD","DF","FC","FB","EB","EA","AD","UF","CF","BB","BE","AU","AA","FF","AF","UC","EE","FA","CC","DD","DU","AE","BF","BA","ED","FE","CB","DC","UU","EU","UA","UB","BC","CD","EF","DB","AB","CU","-1","CA","FU","UE","EC","FD","DE"),
  351 -> Set("U","-1","R","O"))


  def main(args:Array[String]): Unit ={
    val sc = getSparkContext()

    //val otrain = sc.textFile("/Users/jihoonkang/Documents/springleaf/train.csv")

    val otrain = sc.textFile("hdfs://kr-data-h1:9000/user/jihoonkang/springleaf/train.csv")


    val csvTrain = otrain.filter(x => !x.startsWith(""""ID""")).map(x=>{
      val csvdata = new CSVParser().parseLine(x)
      csvdata

    })

/*
   val col = csvTrain.take(100).map(x=>{
      x.zipWithIndex.map(y=>{

        if(y._1.trim.size>=1 && Try{y._1.toDouble}.isFailure){
          if(Try{y._1.toBoolean}.isSuccess){
            (y._2, "Boo")
        } else if(Try{new SimpleDateFormat("ddMMMyy:HH:mm:ss").parse(y._1)}.isSuccess){
            (y._2, "Dat")
        } else {
            (y._2, "Cat")
          }
        }else{
          (y._2, "Num")
        }
      })
    })


    val rc = sc.parallelize(catIdx.toSeq).map(x=>(x,1))

    val ic = csvTrain.map(x=>x.zipWithIndex.map(x=>(x._2,x._1))).flatMap(x=>x).join(rc).map(x=>(x._1,x._2._1)).groupBy(x=>x._1).map(x=>(x._1, x._2.map(x=>x._2).toSet))
    ic.map(x=>s"${x._1},${x._2.mkString(",")}").saveAsTextFile("ic")

    val t = ic.join(rc).map(x=>x._2).distinct().groupBy(x=>x._2).map(x=>(x._1, x._2.map(x=>x._1).mkString(",")))

    t.collect().foreach(x=>{
      println(s"${x._1},${x._2}")
    })

    val lp = csvTrain.map(x=>{
      val vec = x.zipWithIndex.drop(1).dropRight(1).map(v=>{
        if(datIdx.contains(v._2)){

        }else if(booIdx.contains(v._2)){

        }else if(Cat)

      })
      //.map(x=Try{Some(x.toDouble)}.getOrElse(None)).flatten
      LabeledPoint(x.takeRight(1).head.toInt, Vectors.dense(vec))})

    println(lp.take(2).last.features.toArray.mkString(","))


    val otest = sc.textFile("hdfs://kr-data-h1:9000/user/jihoonkang/springleaf/test.csv")
    val csvTest = otest.filter(x => !x.startsWith(""""ID""")).map(x=>{

        val csvdata = new CSVParser().parseLine(x)
      (csvdata.head, Vectors.dense(csvdata.drop(1).map(x => Try {
          x.toDouble
        }.getOrElse(MurmurHash3.stringHash(x).toDouble))))

    })

    val Array(train, test) = csvTrain.randomSplit(Array(0.7, 0.3))
    val numClasses = 2

    val a = train
    //train.take(10).foreach(x=>println(x))

    val categoricalFeaturesInfo = Map[Int, Int]()


    val rfm: RandomForestModel = RandomForest.trainClassifier(lp, 2, categoricalFeaturesInfo, 500, "auto", "gini", 6, 200, 5)
*/




  }

}
