package rknn_outlier_detection

import org.apache.spark._
import rknn_outlier_detection.detection.Techniques
import rknn_outlier_detection.search.ExhaustiveSearch
import rknn_outlier_detection.utils.Utils.readCSV

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

        val FILE_PATH = "datasets/Iris-virginica_Iris-setosa_4.csv"
        val k = 21
        val topN = 6

//        Read data and convert to instances

        val data = readCSV(FILE_PATH, sc)

        val instances = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            new custom_objects.Instance(index.toString, line.slice(0, 4).map(_.toDouble), line.last)
        })

//        Find kNeighbors

        val x = ExhaustiveSearch.getKNeighbors(instances, k)

//        Find reverse neighbors

        val y = ExhaustiveSearch.getReverseNeighbors(x)

//        Add neighbors and reverse neighbors to instances

        val equippedInstances = instances.map(instance => (instance.id, instance))
            .join(x)
            .map(tuple => {
                val (key, nestedTuple) = tuple
                val (instance, kNeighbors) = nestedTuple
                instance.kNeighbors = kNeighbors
                (key, instance)
            })
            .join(y)
            .map(tuple => {
                val (key, nestedTuple) = tuple
                val (instance, rNeighbors) = nestedTuple
                instance.rNeighbors = rNeighbors
                instance
            })

        val antihubValues = Techniques.antihub(y)

        val sortedAntihubValues = antihubValues.sortBy(_._2, ascending = false)
        val anomalousIds = sortedAntihubValues.take(topN).map(tuple => tuple._1)
        println(s"Anomalous instances ${anomalousIds.mkString("{", ", ", "}")}")
    }
}
