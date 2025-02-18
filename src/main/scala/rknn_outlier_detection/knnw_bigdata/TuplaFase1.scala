package rknn_outlier_detection.knnw_bigdata

/** TuplaFase1 es una estructura que se utiliza en el DataSet que se obtiene de la función fase1
 *
 * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
 * @param id      es un Long que representa el identificador único de la tupla
 * @param valores es un arreglo de Double que representa los valores de los atributos de la tupla
 * @param ia      es un Double que representa el índice de anomalía de la tupla
 */
case class TuplaFase1(id: String, valores: Seq[Double], ia: Double) extends Serializable
