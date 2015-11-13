import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable.ArrayBuffer

object DOJA {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "DOJA", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    var r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var s_pairs = S.map { x => var pos = x.indexOf('*')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    //duplication-based outer joins for r_dup and s_loc
    val broadCastR = sc.broadcast(r_pairs.collect())

    var Inner_Join = s_pairs.mapPartitions({ iter =>
      var m = broadCastR.value.iterator
      var cogroup = new CoGroupTable[Long](m, iter)
      cogroup.compute()
    }).flatMapValues({ pair =>
      for (v <- pair(0).iterator; w <- pair(1).iterator) yield (v, w)
    })

    var results2 = r_pairs.cogroup(Inner_Join).flatMapValues { pair =>
      if (pair._2.isEmpty) {
        pair._1.iterator.map(v => (v, None))
      } else {
        for (w <- pair._2.iterator) yield w
      }
    }

    println("Duplication number of result is " + results2.count())
    sc.stop()

  }
}
