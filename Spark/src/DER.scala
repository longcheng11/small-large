import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object DER {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "DER", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))


    // R is smaller, so broadcast it in the form of <k,id|v>
    val r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }.zipWithUniqueId()

    var s_pairs = S.map { x => var pos = x.indexOf('*')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    //DER implementation for the rest part
    val broadCastR = sc.broadcast(r_pairs.map(x => (x._1._1, (x._2 + "|" + x._1._2))).collect())

    val N = s_pairs.partitions.size //number of partitions for final id checking

    // S outer join with broadcast R, the results are in the form of (k,(id|v,some))
    val join_1 = s_pairs.mapPartitions({ iter =>
      var m = broadCastR.value.iterator
      var cogroup = new CoGroupTable[Long](m, iter)
      cogroup.compute()
    }).flatMapValues { pair =>
      if (pair(1).isEmpty) {
        pair(0).iterator.map { v => (v, None)}
      } else {
        for (v <- pair(0).iterator; w <- pair(1).iterator) yield {
          var pos = v.indexOf('|')
          (v.substring(pos + 1), Some(w))
        }
      }
    }

    //the matched results, first filter and then transfer id|v to v
    //var matched = join_1.filter(x => x._2._2 != None).mapValues { v =>
    //  var pos = v._1.indexOf('|')
    //  (v._1.substring(pos + 1), v._2)
    //}
    var matched = join_1.filter(x => x._2._2 != None)
    println("DER number of result1 is " + matched.count())

    //the keys of the non-matched part, first filter and then extract the ids and get the keys with number == N
    var non_id = join_1.filter(x => x._2._2 == None).map { tuple =>
      var pos = tuple._2._1.indexOf('|')
      (tuple._2._1.substring(0, pos).toLong, 1)
    }.reduceByKey(_ + _).filter(key => key._2 == N)


    //based on ids, search the responsible tuples from the broadcast R
    // var non_matched = non_id.mapPartitions({ iter =>
    //  var m = broadCastR.value
    //  for (pos <- iter) yield {
    //   var pair = m.apply(pos._1.toInt)
    //  var p = pair._2.indexOf('|')
    // (pair._1, pair._2.substring(p + 1), None)
    // }
    //})

    //below is an opetion, ids join with R and get the non-matched tuples
    var r_1 = r_pairs.map(x => (x._2, x._1))
    var non_matched = non_id.join(r_1).map(y => y._2._2)
    println("DER number of result2 is " + non_matched.count())

    sc.stop()
  }

}
