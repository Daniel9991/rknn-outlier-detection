package rknn_outlier_detection.search

/*
* This class implements a knn pivot-based search method for distributed environments based
* on the following research paper. TODO fetch paper url
*/

class PkNN {

    // initial n pivots are chosen
    // how to choose the pivots???

    // group instances based on their distances to the pivots
    // creating Voronoi cells

    // each partition needs a support set for the cell so that all neighbors can be found locally

    // Multistep pknn
    // 1. knn for each point of each cell -> core knn
    // 2. find core distance for each cell (max distance of a point to its k-neighbor)
    // 3. find support distance for each cell (max sum of pivot-to-instance distance + instance-to-kneighbor distance)
    // 4. find supporting cells for each cell (all cells for which support distance > half of distance between cells pivots)
    // 5. prune the support sets for each cell, eliminating all support candidates whose
    // rest between the distance (|vi, q| - |vj, q|)/2 >= core-distance(vi)



}
