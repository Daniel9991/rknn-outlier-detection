package rknn_outlier_detection.knnw_bigdata

/** ResultadoBanco es una estructura que se utiliza en el DataSet que retorna la función exce
 *
 * @constructor crea una nueva tupla con su identificador único, un arreglo compuesto por los valores de los atributos de la tupla  y el  índice de anomalías.
 * @param id      es un Long que representa el identificador único de la tupla
 * @param mes     es un entero que representa el dia del mes
 * @param importe es un Double que representa el importe
 * @param ia      es un Double que representa el índice de anomalía de la tupla
 */
case class ResultadoBanco(id: Long, mes: Int, importe: Double, ia: Double) extends Serializable
