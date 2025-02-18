package rknn_outlier_detection.knnw_bigdata

/** TuplaFase1Banco es una estructura que se utiliza en el DataSet que se obtiene de la función fase1Banco
 *
 * @constructor crea una nueva tupla con su identificador único, el dia del mes, el importe y el  índice de anomalías.
 * @param id      es un Long que representa el identificador único de la tupla
 * @param mes     es un entero que representa el dia del mes
 * @param importe es un Double que representa el importe
 * @param ia      es un Double que representa el índice de anomalía de la tupla
 */
case class TuplaFase1Banco(id: Long, mes: Int, importe: Double, ia: Double) extends Serializable
