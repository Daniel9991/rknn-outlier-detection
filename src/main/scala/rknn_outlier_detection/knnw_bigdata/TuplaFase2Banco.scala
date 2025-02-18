package rknn_outlier_detection.knnw_bigdata

/** TuplaFase2 es una estructura que se utiliza en el DataSet en la función fase2Banco
 *
 * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
 * @param id         es un Long que representa el identificador único de la tupla
 * @param mes        es un entero que representa el dia del mes
 * @param importe    es un Double que representa el importe
 * @param distancias es un arreglo de Double que representa las distancias de los k vecinos cercanos de la tupla
 */
case class TuplaFase2Banco(id: Long, mes: Int, importe: Double, distancias: Seq[Double]) extends Serializable
