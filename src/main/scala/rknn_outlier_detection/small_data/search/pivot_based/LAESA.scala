package rknn_outlier_detection.small_data.search.pivot_based

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions

import scala.util.Random

class LAESA(val pivotsAmount: Int){

    val SEED = 35612345
    val r = new Random(SEED)

//    def findPivotsAmount(totalAmount: Int): Int =
//        (totalAmount * pivotsPercentage).toInt

    def findPivots(instances: Array[Instance], pivotsAmount: Int): Array[Instance] = {
        r.shuffle(instances.toList).take(pivotsAmount).toArray

//        Array(instances(0), instances(146), instances(147), instances(148), instances(149))
    }

    def findAllKNeighbors(instances: Array[Instance], k: Int, custom: Boolean = false): Array[Array[KNeighbor]] ={

        // val pivotsAmount = findPivotsAmount(instances.length)
        val pivots = findPivots(instances, pivotsAmount)
        val distances = pivots.map(pivot => instances.map(instance => DistanceFunctions.euclidean(pivot.attributes, instance.attributes)))

        val closestNeighborForAll = instances.map(instance => (
            if(custom) findKNeighborsCustom(instance, instances, pivots, distances, k)
            else findKNeighbors(instance, instances, pivots, distances, k)
        ))
        closestNeighborForAll
    }

    def findAllKNeighborsForBenchmark(instances: Array[Instance], k: Int, custom: Boolean = false): (Array[Array[KNeighbor]], Int, Int) ={

        // val pivotsAmount = findPivotsAmount(instances.length)
        val pivots = findPivots(instances, pivotsAmount)
        val distances = pivots.map(pivot => instances.map(instance => DistanceFunctions.euclidean(pivot.attributes, instance.attributes)))

        val results = instances.map(instance => (
            if(custom) findKNeighborsCustomForBenchmark(instance, instances, pivots, distances, k)
            else findKNeighborsForBenchmark(instance, instances, pivots, distances, k)
            ))

        val closestNeighborsForAll = results.map(_._1)
        val distanciasTotales = results.map(_._2).sum
        val eliminacionesTotales = results.map(_._3).sum

        (closestNeighborsForAll, distanciasTotales, eliminacionesTotales)
    }

    def findKNeighbors(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]],
        k: Int,
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

//            println("-------------/----\n")
        }

        kNeighbors
    }

    def findKNeighborsCustom(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]],
        k: Int,
    ): Array[KNeighbor] = {

        //        println(s"Query is: ${query.id}")
        val nonPivots = instances.filter(instance => !pivots.contains(instance))
        val kNeighbors = Array.fill(k)(null.asInstanceOf[KNeighbor])
        var radius = Double.PositiveInfinity
//        var s = pivots(0)
//        var sIndexAsInst = instances.indexOf(s)
//        var sIndexAsPivot = 0

        // matching instances by index
        val cotas = instances.map(_ => 0d)

        // track whether or not an instance has been analyzed through false or
        // true value according to index
        val analyzedInstances = instances.map(_ => false)

        // TODO: Make pivots be analyzed first so that all instances
        //       are as approximate as possible (cotas array)

        for(s <- pivots){
            val sIndexAsInst = instances.indexOf(s)
            val sIndexAsPivot = pivots.indexOf(s)
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

                    val difference = distances(sIndexAsPivot)(i) - dist
                    cotas(i) = Math.max(cotas(i), Math.abs(difference))

                    // eliminacion
                    if(cotas(i) >= radius){
                        //                        println(s"Eliminating ${u.id} with cota ${cotas(i)} and radius $radius")
                        analyzedInstances(i) = true
                    }
                }
            }
            //            println("-----------------\n")
        }

        var s = nonPivots(0)
        var sIndexAsInst = instances.indexOf(s)

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

            var g = Double.PositiveInfinity

            for(i <- instances.indices){
                if(!analyzedInstances(i)){ // Si esta es una instancia que hay que analizar
                    val u = instances(i)

                    // eliminacion
                    if(cotas(i) >= radius){
                        //                        println(s"Eliminating ${u.id} with cota ${cotas(i)} and radius $radius")
                        analyzedInstances(i) = true
                    }
                    // aproximacion
                    else{
                        if(cotas(i) < g){
                            g = cotas(i)
                            s = u
                            sIndexAsInst = i
                        }
                    }
                }
            }
            //            println("-----------------\n")
        }

        kNeighbors
    }

    def findKNeighborsForBenchmark(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]],
        k: Int,
    ): (Array[KNeighbor], Int, Int) = {

        //        println(s"Query is: ${query.id}")
        var distancias = 0
        var eliminaciones = 0
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
            distancias += 1
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
                        eliminaciones += 1
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

        (kNeighbors, distancias, eliminaciones)
    }

    def findKNeighborsCustomForBenchmark(
        query: Instance,
        instances: Array[Instance],
        pivots: Array[Instance],
        distances: Array[Array[Double]],
        k: Int,
    ): (Array[KNeighbor], Int, Int) = {

        //        println(s"Query is: ${query.id}")
        var distancias = 0
        var eliminaciones = 0
        val nonPivots = instances.filter(instance => !pivots.contains(instance))
        val kNeighbors = Array.fill(k)(null.asInstanceOf[KNeighbor])
        var radius = Double.PositiveInfinity
        //        var s = pivots(0)
        //        var sIndexAsInst = instances.indexOf(s)
        //        var sIndexAsPivot = 0

        // matching instances by index
        val cotas = instances.map(_ => 0d)

        // track whether or not an instance has been analyzed through false or
        // true value according to index
        val analyzedInstances = instances.map(_ => false)

        // TODO: Make pivots be analyzed first so that all instances
        //       are as approximate as possible (cotas array)

        for(s <- pivots){
            val sIndexAsInst = instances.indexOf(s)
            val sIndexAsPivot = pivots.indexOf(s)
            val dist = DistanceFunctions.euclidean(s.attributes, query.attributes)
            distancias += 1
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

                    val difference = distances(sIndexAsPivot)(i) - dist
                    cotas(i) = Math.max(cotas(i), Math.abs(difference))

                    // eliminacion
                    if(cotas(i) >= radius){
                        //                        println(s"Eliminating ${u.id} with cota ${cotas(i)} and radius $radius")
                        analyzedInstances(i) = true
                        eliminaciones += 1
                    }
                }
            }
            //            println("-----------------\n")
        }

        var s = nonPivots(0)
        var sIndexAsInst = instances.indexOf(s)

        // Mientras haya instancias que no hayan sido analizadas
        while(analyzedInstances.contains(false)){
            val dist = DistanceFunctions.euclidean(s.attributes, query.attributes)
            distancias += 1
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

            var g = Double.PositiveInfinity

            for(i <- instances.indices){
                if(!analyzedInstances(i)){ // Si esta es una instancia que hay que analizar
                    val u = instances(i)

                    // eliminacion
                    if(cotas(i) >= radius){
                        //                        println(s"Eliminating ${u.id} with cota ${cotas(i)} and radius $radius")
                        analyzedInstances(i) = true
                        eliminaciones += 1
                    }
                    // aproximacion
                    else{
                        if(cotas(i) < g){
                            g = cotas(i)
                            s = u
                            sIndexAsInst = i
                        }
                    }
                }
            }
            //            println("-----------------\n")
        }

        (kNeighbors, distancias, eliminaciones)
    }

    /**
     * Add a new KNeighbor to an array.
     * If the array contains empty spots i.e. there is a position that
     * contains null, find the index of the position and insert the neighbor there.
     * If the array is full, insert neighbor in the last position.
     *
     * After insertion, have new neighbor work its way down from the insertion
     * position as necessary comparing itself with the neighbor below (or ot its left).
     *
     * @param kNeighbors Array of KNeighbors that can contain null spots. Expected to be sorted
     * @param newNeighbor KNeighbor to insert in array
     * @return Unit - The array is modified in place
     */
    def addNewNeighbor(
      kNeighbors: Array[KNeighbor],
      newNeighbor: KNeighbor
    ): Unit = {

        var currentIndex: Int = 0

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


