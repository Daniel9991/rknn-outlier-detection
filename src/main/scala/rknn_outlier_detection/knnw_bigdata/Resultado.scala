package rknn_outlier_detection.knnw_bigdata

/** Resultado es una estructura que se utiliza en el DataSet que retorna la función exce
 *
 * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
 * @param ID es un Long que representa el identificador único de la tupla
 * @param ia es un Double que representa el índice de anomalía de la tupla
 */
case class Resultado(ID: String, ia: Double, data: Seq[Double]) extends Serializable
