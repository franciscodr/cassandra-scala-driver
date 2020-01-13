package io.cassandra.example.http.codecs

import cats.effect.IO
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityEncoder, MediaType}

import scala.xml.{NamespaceBinding, NodeSeq, PrettyPrinter}

trait XmlEncoders {
  val w3orgScope: NamespaceBinding = scalaxb.toScope(
    Some("xs") -> "http://www.w3.org/2001/XMLSchema",
    Some("xsi") -> "http://www.w3.org/2001/XMLSchema-instance"
  )

  implicit val nodeSeqEntityEncoder: EntityEncoder[IO, NodeSeq] = {
    val pp = new PrettyPrinter(1, 1, false)
    EntityEncoder
      .stringEncoder[IO]
      .contramap[NodeSeq](n => pp.formatNodes(n, w3orgScope))
      .withContentType(`Content-Type`(MediaType.application.xml))
  }
}
