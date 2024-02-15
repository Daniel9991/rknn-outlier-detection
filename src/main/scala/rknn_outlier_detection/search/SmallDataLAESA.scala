package rknn_outlier_detection.search

import rknn_outlier_detection.custom_objects.Instance
import rknn_outlier_detection.distance.DistanceFunctions

import scala.util.Random

class SmallDataLAESA(val pivotsPercentage: Double){

    val SEED = 35612345
    val r = new Random(SEED)

    def findPivotsAmount(totalAmount: Int): Int =
        (totalAmount * pivotsPercentage).toInt

    def findPivots(instances: Array[Instance], pivotsAmount: Int): Array[Instance] =
        r.shuffle(instances.toList).take(pivotsAmount).toArray

    def findAllKNeighbors(instances: Array[Instance]): Array[Instance] ={

        val pivotsAmount = findPivotsAmount(instances.length)
        val pivots = findPivots(instances, pivotsAmount)
        val distances = pivots.map(pivot => instances.map(instance => DistanceFunctions.euclidean(pivot.attributes, instance.attributes)))

        val closestNeighborForAll = instances.map(instance => findKNeighbors(instance, instances, pivots, distances))
        closestNeighborForAll.map(neighbors => neighbors(0))
    }

    def findKNeighbors(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]]
    ): Array[Instance] = {

        var neighbor: Instance = null
        var radius = Double.PositiveInfinity
        var s = pivots(0)
        var sIndexAsInst = instances.indexOf(s)
        var sIndexAsPivot = 0

        // matching instances by index
        val cotas = instances.map(_ => 0d)

        // track whether or not an instance has been analyzed through false or
        // true value according to index
        val analyzedInstances = instances.map(_ => false)

        // Mientras haya instancias que no hayan sido analizadas
        while(analyzedInstances.contains(false)){
            val dist = DistanceFunctions.euclidean(s.attributes, query.attributes)
            analyzedInstances(sIndexAsInst) = true

            if(dist < radius){
                neighbor = s
                radius = dist
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
                        analyzedInstances(i) = true
                    }
                    // aproximacion
                    else{
                        if(pivots.contains(u)){
                            if(cotas(i) < gB){
                                gB = cotas(i)
                                sigB = u
                            }
                        }
                        else{
                            if(cotas(i) < g){
                                g = cotas(i)
                                sig = u
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
        }

        Array(neighbor)
    }

}


