package rknn_outlier_detection.knnw_bigdata

/** TuplaFase2 es una estructura que se utiliza en el DataSet en la función fase2
 *
 * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
 * @param id         es un Long que representa el identificador único de la tupla
 * @param valores    es un arreglo de Double que representa los valores de los atributos de la tupla
 * @param distancias es un arreglo de Double que representa las distancias de los k vecinos cercanos de la tupla
 */
case class TuplaFase2(id: String, valores: Seq[Double], distancias: Seq[Double]) extends Serializable
