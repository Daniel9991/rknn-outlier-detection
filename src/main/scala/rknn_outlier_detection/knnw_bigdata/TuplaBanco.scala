package rknn_outlier_detection.knnw_bigdata

/** TuplaBanco es una estructura que se utiliza en los DataSets
 *
 * @constructor crea una nueva tupla con su identificador, el dia del mes y el importe.
 * @param id      es un Long que representa el identificador único de la Tupla
 * @param mes     es un entero que representa el dia del mes
 * @param importe es un Double que representa el importe
 */
case class TuplaBanco(id: Long, mes: Int, importe: Double) extends Serializable
