package rknn_outlier_detection.knnw_bigdata

/** Tupla es una estructura que se utiliza en los DataSets
 *
 * @constructor crea una nueva Tupla con su identificador y un arreglo compuesto por los valores de los atributos de la tupla.
 * @param id      es un Long que representa el identificador único de la Tupla
 * @param valores es un arreglo de Double que representa los valores de los atributos de la tupla
 */
case class Tupla(id: String, valores: Array[Double]) extends Serializable
