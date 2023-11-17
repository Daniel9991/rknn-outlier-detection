package rknn_outlier_detection.detection

// Case class ??? But it generates a bunch of unnecessary methods
class AntihubRefinedParams (val step: Double, val ratio: Double){
    if(step <= 0){
        throw new Exception("step parameter must be greater than 0")
    }

    if(step > 1){
        throw new Exception("step parameter must be lesser or equal to 1")
    }

    if(ratio <= 0){
        throw new Exception("ratio parameter must be greater than 0")
    }

    if(ratio > 1){
        throw new Exception("ratio parameter must be lesser or equal to 0")
    }
}
