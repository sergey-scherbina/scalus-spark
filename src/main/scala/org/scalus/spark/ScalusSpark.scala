package org.scalus.spark

import io.bullet.*
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BinaryEncoder, TransformingEncoder}
import org.apache.spark.sql.catalyst.encoders.Codec
import scalus.builtin.ByteString
import scalus.cardano.ledger.*

import scala.reflect.{ClassTag, classTag}

extension (spark: SparkSession)
  def readCardanoCbor(paths: String*): Dataset[BlockFile] =
    spark.read.format("binaryFile").load(paths *)
      .map(_.getAs[Array[Byte]]("content"))
      .map(readBlockFileCbor)

def readBlockFileCbor(bytes: Array[Byte]): BlockFile = {
  given OriginalCborByteArray = OriginalCborByteArray(bytes)

  borer.Cbor.decode(bytes).to[BlockFile].value
}

def readBlockCbor(bytes: Array[Byte]): Block = {
  given OriginalCborByteArray = OriginalCborByteArray(bytes)

  borer.Cbor.decode(bytes).to[Block].value
}

object BlockFileCodec extends Codec[BlockFile, Array[Byte]], Serializable {
  override def encode(in: BlockFile): Array[Byte] = borer.Cbor.encode(in).toByteArray
  override def decode(out: Array[Byte]): BlockFile = readBlockFileCbor(out)
}

object BlockCodec extends Codec[Block, Array[Byte]], Serializable {
  override def encode(in: Block): Array[Byte] = borer.Cbor.encode(in).toByteArray
  override def decode(out: Array[Byte]): Block = readBlockCbor(out)
}

class CborCodec[T: {borer.Encoder, borer.Decoder}] extends Codec[T, Array[Byte]], Serializable {
  override def encode(in: T): Array[Byte] = borer.Cbor.encode(in).toByteArray
  override def decode(out: Array[Byte]): T = borer.Cbor.decode(out).to[T].value
}

class HashEncoder[F, P] extends borer.Encoder[Hash[F, P]], Serializable {
  override def write(w: borer.Writer, value: Hash[F, P]): borer.Writer =
    w.writeBytes(value.bytes)
}

class HashDecoder[F: HashSize, P] extends borer.Decoder[Hash[F, P]], Serializable {
  override def read(r: borer.Reader): Hash[F, P] = Hash[F, P]:
    ByteString.unsafeFromArray(r.readBytes())
}

given [T: {ClassTag, borer.Encoder, borer.Decoder}]: Encoder[T] =
  TransformingEncoder(classTag[T], BinaryEncoder, () => new CborCodec[T])

given Encoder[Block] = TransformingEncoder(classTag[Block], BinaryEncoder, () => BlockCodec)

given Encoder[BlockFile] = TransformingEncoder(classTag[BlockFile], BinaryEncoder, () => BlockFileCodec)

given [F, P]: borer.Encoder[Hash[F, P]] = HashEncoder[F, P]

given [F: HashSize, P]: borer.Decoder[Hash[F, P]] = HashDecoder[F, P]
