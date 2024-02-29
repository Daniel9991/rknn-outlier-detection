package rknn_outlier_detection.search.small_data

import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.DistanceFunctions

import scala.util.Random

class LAESA(val pivotsAmount: Integer){

    val SEED = 35612345
    val r = new Random(SEED)

//    def findPivotsAmount(totalAmount: Int): Int =
//        (totalAmount * pivotsPercentage).toInt

    def findPivots(instances: Array[Instance], pivotsAmount: Int): Array[Instance] =
        r.shuffle(instances.toList).take(pivotsAmount).toArray

    def findAllKNeighbors(instances: Array[Instance], k: Integer): Array[Array[KNeighbor]] ={

        // val pivotsAmount = findPivotsAmount(instances.length)
        val pivots = findPivots(instances, pivotsAmount)
        val distances = pivots.map(pivot => instances.map(instance => DistanceFunctions.euclidean(pivot.attributes, instance.attributes)))

        val closestNeighborForAll = instances.map(instance => findKNeighbors(instance, instances, pivots, distances, k))
        closestNeighborForAll
    }

    def findKNeighbors(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]],
        k: Integer,
    ): Array[KNeighbor] = {

//        println(s"Query is: ${query.id}")
        val kNeighbors = Array.fill(k)(null.asInstanceOf[KNeighbor])
        var radius = Double.PositiveInfinity
        var s = pivots(0)
        var sIndexAsInst = instances.indexOf(s)
        var sIndexAsPivot = 0

        // matching instances by index
        val cotas = instances.map(_ => 0d)

        // track whether or not an instance has been analyzed through false or
        // true value according to index
        val analyzedInstances = instances.map(_ => false)

        // TODO: Make pivots be analyzed first so that all instances
        //       are as approximate as possible (cotas array)

        // Mientras haya instancias que no hayan sido analizadas
        while(analyzedInstances.contains(false)){
            val dist = DistanceFunctions.euclidean(s.attributes, query.attributes)
            analyzedInstances(sIndexAsInst) = true

//            println(s"Analyzing instance ${s.id} with distance ${dist}")
            if(kNeighbors.contains(null) || dist < radius){
                if(s.id != query.id){
//                    println("Goes in")
                    addNewNeighbor(kNeighbors, new KNeighbor(s.id, dist))

                    if(!kNeighbors.contains(null))
                        radius = kNeighbors.last.distance
                }
            }

            var sigB: Instance = null
            var gB = Double.PositiveInfinity
            var sig: Instance = null
            var g = Double.PositiveInfinity

            for(i <- instances.indices){
                if(!analyzedInstances(i)){ // Si esta es una instancia que hay que analizar
                    val u = instances(i)

                    if(pivots.contains(s)){
                        val difference = distances(sIndexAsPivot)(i) - dist
                        cotas(i) = Math.max(cotas(i), Math.abs(difference))
                    }

                    // eliminacion
                    if(cotas(i) >= radius){
//                        println(s"Eliminating ${u.id} with cota ${cotas(i)} and radius $radius")
                        analyzedInstances(i) = true
                    }
                    // aproximacion
                    else{
                        if(pivots.contains(u)){
                            if(cotas(i) < gB){
                                gB = cotas(i)
                                sigB = u
                                sIndexAsInst = instances.indexOf(sigB)
                                sIndexAsPivot = pivots.indexOf(sigB)
                            }
                        }
                        else{
                            if(cotas(i) < g){
                                g = cotas(i)
                                sig = u
                                sIndexAsInst = instances.indexOf(sig)
                            }
                        }
                    }
                }
            }

            if(sigB != null){
                s = sigB
            }
            else{
                s = sig
            }

//            println("-----------------\n")
        }

        kNeighbors
    }

    /**
     * Find k neighbors by using the cartesian product to get all
     * combinations of instances.
     * Filter out pairs where both instances are the same.
     * Map all pairs to KNeighbor objects
     * Group all neighbors by instance
     * Sort all neighbors for an instance and slice it to get
     * the k closest instances
     *
     * Costly as it produces lots of pairs (n squared).
     * Sorts n arrays of size n-1 to get k neighbors (n being instances length)
     *
     * @param kNeighbors Array of KNeighbors that can contain null spots. Expected to be sorted
     * @param newNeighbor KNeighbor to insert in array
     * @return Unit - The array is modified in place
     */
    def addNewNeighbor(
      kNeighbors: Array[KNeighbor],
      newNeighbor: KNeighbor
    ): Unit = {

        var currentIndex: Integer = 0

        // If array contains null, insert in first null position
        if(kNeighbors.contains(null)){
            currentIndex = kNeighbors.indexOf(null)
            kNeighbors(currentIndex) = newNeighbor
        }
        // If array is full, insert in last position
        else {
            currentIndex = kNeighbors.length - 1
            kNeighbors(currentIndex) = newNeighbor
        }

        while (newNeighbor != kNeighbors.head &&
            newNeighbor.distance < kNeighbors(currentIndex - 1).distance) {
            kNeighbors(currentIndex) = kNeighbors(currentIndex - 1)
            currentIndex -= 1
            kNeighbors(currentIndex) = newNeighbor
        }
    }
}


