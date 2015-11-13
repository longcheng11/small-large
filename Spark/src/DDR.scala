import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object DDR {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "DDR", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    // R is smaller, so broadcast it as a map<String, String>
    var r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var s_pairs = S.map { x => var pos = x.indexOf('*')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    //DER implementation for the rest part
    val broadCastR = sc.broadcast(r_pairs.collect())

    val N = s_pairs.partitions.size //number of partitions for final id checking

    // S outer join with broadcast R
    val join_1 = s_pairs.mapPartitions({ iter =>
      var m = broadCastR.value.iterator
      var cogroup = new CoGroupTable[Long](m, iter)
      cogroup.compute()
    }).flatMapValues { pair =>
      if (pair(1).isEmpty) {
        pair(0).iterator.map(v => (v, None))
      } else {
        for (v <- pair(0).iterator; w <- pair(1).iterator) yield (v, Some(w))
      }
    }

    //the matched results
    var matched = join_1.filter(x => x._2._2 != None)
    println("DDR number of result1 is " + matched.count())

    //the keys of the non-matched part, and get the keys with number == N
    var non_matched = join_1.filter(x => x._2._2 == None).map(tuple => ((tuple._1, tuple._2._1), 1)).reduceByKey(_ + _).filter(key => key._2 == N).map(y => y._1)
    println("DDR number of result2 is " + non_matched.count())

    sc.stop()
  }

}
