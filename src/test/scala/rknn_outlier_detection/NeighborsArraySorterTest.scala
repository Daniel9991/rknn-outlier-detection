package rknn_outlier_detection

import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.custom_objects.KNeighbor
import rknn_outlier_detection.utils.Utils

class NeighborsArraySorterTest extends AnyFunSuite {

    val neighbor1 = new KNeighbor("1", 1.4678)
    val neighbor2 = new KNeighbor("2", 2.9184)
    val neighbor3 = new KNeighbor("3", 3.5643)
    val neighbor4 = new KNeighbor("4", 3.5643)
    val neighbor5 = new KNeighbor("5", 5.9023)
    val neighbor6 = new KNeighbor("6", 5.9024)
    val neighbor7 = new KNeighbor("7", 8.2151)
    val neighbor8 = new KNeighbor("8", 10.1234)


    test("Testing array length"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
    }

    test("Insert 1 neighbor"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        assert(Utils.arrayEquals(array1, Array[KNeighbor](neighbor4, null, null, null, null), array1.length))
    }

    test("Insert 2 neighbors"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        val array2 = Utils.insertNeighborInArray(array1, neighbor7)
        assert(Utils.arrayEquals(array2, Array[KNeighbor](neighbor4, neighbor7, null, null, null), array2.length))
    }

    test("Insert 3 neighbors"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        val array2 = Utils.insertNeighborInArray(array1, neighbor7)
        val array3 = Utils.insertNeighborInArray(array2, neighbor2)
        assert(Utils.arrayEquals(array3, Array[KNeighbor](neighbor2, neighbor4, neighbor7, null, null), array3.length))
    }

    test("Insert same distance neighbor"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        val array2 = Utils.insertNeighborInArray(array1, neighbor7)
        val array3 = Utils.insertNeighborInArray(array2, neighbor2)
        val array4 = Utils.insertNeighborInArray(array3, neighbor3)
        assert(Utils.arrayEquals(array4, Array[KNeighbor](neighbor2, neighbor4, neighbor3, neighbor7, null), array4.length))
    }

    test("Insert 5 neighbors"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        val array2 = Utils.insertNeighborInArray(array1, neighbor7)
        val array3 = Utils.insertNeighborInArray(array2, neighbor2)
        val array4 = Utils.insertNeighborInArray(array3, neighbor3)
        val array5 = Utils.insertNeighborInArray(array4, neighbor8)
        assert(Utils.arrayEquals(array5, Array[KNeighbor](neighbor2, neighbor4, neighbor3, neighbor7, neighbor8), array5.length))
    }

    test("Insert into full array"){
        val array = Array[KNeighbor](null, null, null, null, null)
        assert(array.length == 5)
        val array1 = Utils.insertNeighborInArray(array, neighbor4)
        val array2 = Utils.insertNeighborInArray(array1, neighbor7)
        val array3 = Utils.insertNeighborInArray(array2, neighbor2)
        val array4 = Utils.insertNeighborInArray(array3, neighbor3)
        val array5 = Utils.insertNeighborInArray(array4, neighbor8)
        val array6 = Utils.insertNeighborInArray(array5, neighbor1)
        assert(Utils.arrayEquals(array6, Array[KNeighbor](neighbor1, neighbor2, neighbor4, neighbor3, neighbor7), array6.length))
    }
}
