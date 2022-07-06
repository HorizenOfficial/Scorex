package scorex.core.network.dns.strategy

import scorex.core.network.PeerSynchronizer.ReceivableMessages.LookupResponse
import scorex.core.network.dns.DnsClient.Exception.DnsLookupException
import scorex.core.network.dns.model.DnsClientInput

import java.net.{Inet4Address, Inet6Address, InetAddress}
import scala.util.{Failure, Success, Try}

trait LookupStrategy {
  def apply(dnsClientParams: DnsClientInput): Try[LookupResponse]
}

object Strategy {
  case class LeastNodeQuantity() extends LookupStrategy {
    private val LEAST_NODE_QUANTITY = 1

    override def apply(dnsClientParams: DnsClientInput): Try[LookupResponse] = thresholdNodeQuantity(dnsClientParams, LEAST_NODE_QUANTITY)
  }

  case class MaxNodeQuantity() extends LookupStrategy {
    private val MAX_NODE_QUANTITY = Int.MaxValue

    override def apply(dnsClientParams: DnsClientInput): Try[LookupResponse] = {
      thresholdNodeQuantity(dnsClientParams, MAX_NODE_QUANTITY)
    }
  }

  case class ThresholdNodeQuantity(nodeThreshold: Int) extends LookupStrategy {
    override def apply(dnsClientParams: DnsClientInput): Try[LookupResponse] = thresholdNodeQuantity(dnsClientParams, nodeThreshold)
  }

  private def thresholdNodeQuantity(dnsClientParams: DnsClientInput, nodesThreshold: Int): Try[LookupResponse] = {
    if (nodesThreshold <= 0) {
      Failure(new IllegalArgumentException("The nodes threshold must be greater than 0"))
    } else {
      val seeders = dnsClientParams.dnsSeeders
      val lookupFunction = dnsClientParams.lookupFunction

      val exceptionMessages = Seq[String]()

      val (ipv4Addresses, ipv6Addresses) = seeders.foldLeft(Seq[InetAddress]()) { (acc, curr) =>
        val newElements = if (acc.size < nodesThreshold) {
          lookupFunction(curr) match {
            case Success(value) => value
            case Failure(exception) =>
              exceptionMessages ++ exception.getMessage
              Seq[InetAddress]()
          }
        } else {
          Seq[InetAddress]()
        }

        acc ++ newElements
      }.partition {
        case _: Inet4Address => true
        case _: Inet6Address => false
      }

      if (ipv4Addresses.isEmpty && ipv6Addresses.isEmpty) {
        Failure(DnsLookupException("All dns lookups failed: " + exceptionMessages.mkString(", ")))
      } else {
        Success(LookupResponse(ipv4Addresses, ipv6Addresses))
      }
    }
  }
}