package com.spark


import java.io.{FileWriter, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ArrayBuffer


/**
 * Created by Utopia on 2016/4/21.
 * args:
 * args[0]  --minSupport
 * args[1]  --DB
 * args[2]  --db
 * args[3]  --DBmidResult
 * args[4]  --Result
 * args[5]  --FinalRecord
 */
object FUP {
  def main(args: Array[String]) {

    val conf_hdfs = new Configuration()
    val hdfs = new Path(args(3))
    val fs = FileSystem.get(URI.create(args(3)),conf_hdfs)
    val status = fs.listStatus(hdfs).length



    val conf = new SparkConf()
    conf.setAppName("AprioriOnSpark")
    //conf.setMaster("local")
    val sc = new SparkContext(conf)

    val minSupport = args(0).toDouble
    val startTime = System.currentTimeMillis()


    val lines_DB = sc.textFile(args(1))
    val DB = lines_DB.map(_.split(" ").map(each =>SortedSet(each.toInt)).reduce( _ ++ _)).cache()
    val DB_num = DB.count()
    val DB_minSupport = ( DB_num * minSupport).toInt

    val lines_db = sc.textFile(args(2))
    val db = lines_db.map(_.split(" ").map(each =>SortedSet(each.toInt)).reduce( (x,y) => x ++ y)).cache()
    val db_num = db.count()
    val db_minSupport =( db_num * minSupport).toInt


    var sign = 1
    var AL_k = Array[(SortedSet[Int], Int)]()
    var C_k = Array[SortedSet[Int]]()
    var L_k_1 = Array[(SortedSet[Int], Int)]()
    var AL_k_1 = Array[(SortedSet[Int], Int)]()
    var L_k = Array[(SortedSet[Int], Int)]()


    do{

      AL_k_1 = AL_k

      L_k_1 = L_k

      if(sign <= status)
        L_k = sc.objectFile(args(3) + sign.toString).collect()
      else
        L_k = Array[(SortedSet[Int], Int)]()


      if (sign == 1) {
        val db_oneItem = lines_db.flatMap(_.split(" ").map(word => SortedSet(word.toInt))).cache()
        val L_k_broadcast = sc.broadcast(L_k)
        L_k = L_k_broadcast.value
        AL_k = FUP_Gen(db_oneItem, L_k, C_k, DB, DB_minSupport, db_minSupport, sign, sc, args(4) + sign.toString)
      }
      else {
        val temp1 = AL_k_1.map(_._1)
        val temp2 = L_k.map(_._1)
        C_k = AprioriGen(temp1, temp2)
        L_k = Cut_Lk(L_k, L_k_1, AL_k_1)
        val L_k_broadcast = sc.broadcast(L_k)
        val C_k_broadcast = sc.broadcast(C_k)
        L_k = L_k_broadcast.value
        C_k = C_k_broadcast.value
        AL_k = FUP_Gen(db, L_k, C_k, DB, DB_minSupport, db_minSupport, sign, sc, args(4) + sign.toString)
        L_k_broadcast.unpersist()
        C_k_broadcast.unpersist()
      }

      sign = sign + 1

    }while(!AL_k.isEmpty)

    val time = (System.currentTimeMillis() - startTime).toString
    val out = new FileWriter(args(5),true)
    AL_k_1.foreach(word =>out.write(word.toString() + "\n"))
    out.write(time + "\n")
    out.flush()
    out.close()

  }


  def AprioriGen(AL_k: Array[SortedSet[Int]] , L_k:Array[SortedSet[Int]]): Array[SortedSet[Int]] = {

    val Ck = ArrayBuffer[SortedSet[Int]]()
    AL_k.foreach{
      l1 =>
        AL_k.foreach{
          l2 =>
            if( l1.init == l2.init && l1.last < l2.last){
              val c = l1.init + l1.last + l2.last
              if(has_infrequent_subset(c,AL_k) && !in_DB_frequent(c,L_k)){
                Ck += c
              }
            }
        }
    }
    Ck.toArray
  }

  def has_infrequent_subset(candidate : SortedSet[Int] , AL_k : Array[SortedSet[Int]]): Boolean ={
    candidate.subsets(candidate.size -1).foreach{
      s =>
        if(AL_k.contains(s))
          return true
    }
    return false
  }

  def in_DB_frequent(candidate : SortedSet[Int] , L_k : Array[SortedSet[Int]]): Boolean ={
    if(L_k.contains(candidate))
      true
    else
      false
  }

  def Cut_Lk(L_k: Array[(SortedSet[Int], Int)], L_k_1: Array[(SortedSet[Int], Int)],
             AL_k_1: Array[(SortedSet[Int], Int)]): Array[(SortedSet[Int], Int)] = {
    val temp = L_k_1.filter{
      s1 =>
        var in = true
        AL_k_1.foreach{
          s2 =>
            if(s1._1 == s2._1)
              in = false
        }
        in
    }
    L_k.filter{
      s =>
        var in = true
        temp.foreach{
          s1 =>
            if(s._1 == s1._1)
              in = false
        }
        in
    }
  }

  def FUP_Gen(db : RDD[SortedSet[Int]] , L_k : Array[(SortedSet[Int] , Int)] , C_k : Array[SortedSet[Int]] ,
              DB : RDD[SortedSet[Int]] ,
              DB_minSupport : Long , db_minSupport : Long , tag : Int , sc : SparkContext,
              path : String): Array[(SortedSet[Int] , Int)] ={


    var result_Array = Array[(SortedSet[Int] , Int)]()

    if(tag == 1){
      val part1 = db.flatMap {
        s =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          L_k.foreach{
            s1 =>
              if(s1._1.subsetOf(s)){
                val t = (s1._1 , 1)
                temp += t
              }
          }
          temp
      }.union(sc.parallelize(L_k)).reduceByKey(_ + _).filter(_._2 >= (DB_minSupport + db_minSupport))

      val part2 = db.map(word => (word , 1)).reduceByKey(_ + _).filter{
        s =>
          var temp = true
          L_k.foreach{
            s2 =>
              if(s2._1.subsetOf(s._1))
                temp = false
          }
          temp
      }.filter(_._2 >=  db_minSupport).collect()

      val bc_part2 = sc.broadcast(part2).value

      val part3 = DB.flatMap{
        s =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          bc_part2.foreach{
            s3 =>
              if(s3._1.subsetOf(s)){
                val t = (s3._1 , 1)
                temp += t
              }
          }
          temp
      }.union(sc.parallelize(bc_part2)).reduceByKey(_ + _).filter(_._2 >= (DB_minSupport + db_minSupport))

      val result = part1 ++ part3

      result.saveAsObjectFile(path)

      result_Array = result.collect()

    }
    else {
      val part1 = db.flatMap {
        s =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          L_k.foreach{
            s1 =>
              if(s1._1.subsetOf(s)){
                val t = (s1._1 , 1)
                temp += t
              }
          }
          temp
      }.union(sc.parallelize(L_k)).reduceByKey(_ + _).filter(_._2 >= (DB_minSupport + db_minSupport))
      val part2 = db.flatMap {
        s =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          C_k.foreach{
            s2 =>
              if(s2.subsetOf(s)){
                val t = (s2 , 1)
                temp += t
              }
          }
          temp
      }.reduceByKey(_ + _).filter(_._2 >= db_minSupport).collect()

      val bc_part2 = sc.broadcast(part2).value

      val part3 = DB.flatMap{
        s =>
          val temp = ArrayBuffer[(SortedSet[Int] , Int)]()
          bc_part2.foreach{
            s3 =>
              if(s3._1.subsetOf(s)){
                val t = (s3._1 , 1)
                temp += t
              }
          }
          temp
      }.union(sc.parallelize(bc_part2)).reduceByKey(_ + _).filter(_._2 >= (DB_minSupport + db_minSupport))


      val result = part1.union(part3)

      result.saveAsObjectFile(path)

      result_Array = result.collect()
    }

    result_Array
  }

}