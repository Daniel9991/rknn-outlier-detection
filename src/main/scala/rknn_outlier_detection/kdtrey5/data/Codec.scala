package rknn_outlier_detection.kdtrey5.data

trait Codec[A, B] extends Encoder[A, B] with Decoder[B, A]

trait Encoder[A, B] {
  def encode(a: A): B
}

trait Decoder[A, B] {
  def decode(a: A): B
}
