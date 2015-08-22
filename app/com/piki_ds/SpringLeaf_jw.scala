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
object SpringLeaf_jw {


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

  val delIdx = Set(221,216)

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

    val otrain = sc.textFile("hdfs://kr-data-h1:9000/user/jihoonkang/springleaf/train.csv")


    val csvTrain = otrain.filter(x => !x.startsWith(""""ID""")).map(x=>{
      val csvdata = new CSVParser().parseLine(x)
      csvdata
    })

    createDummy(csvTrain, deleteOneItem(catVal))
  }


  /**
   * 더미변수로 만들 때 나머지 하나는 고려할 필요가 없음 (0,0,0,0,...) 으로 처리하면 되기 때문에
   * @param catValInfo : 기존의 catVal
   * @return : 하나의 item이 빠진 상태
   */
  def deleteOneItem(catValInfo:Map[Int, Set[String]]) = {
    catValInfo.filter(x=> !delIdx.contains(x._1)).map(x=>(x._1, x._2.toArray)).map(x=>(x._1,x._2.drop(1)))
  }


  def createDummy(filtered:RDD[Array[String]], newCatValInfo: Map[Int, Array[String]]) = {
    filtered.map(x=>{
      val dummies = newCatValInfo.map(a=>{
        val inner = a._2.map(b=> if (b.equals(x(a._1))) b -> 1 else b -> 0)
        (a._1, inner)
      })
      val flattenDummy = dummies.map(a=>a._2.map(b=>(a._1.toString+"_"+b._1,b._2))).flatMap(x=>x)
      (x, flattenDummy)
    })
  }


}
