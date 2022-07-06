package scorex.core.network.dns.model

import scorex.core.network.dns.model.DnsClientInput.defaultLookupFunction

import java.net.InetAddress
import scala.util.Try

case class DnsClientInput(dnsSeeders: Seq[DnsSeederDomain], lookupFunction: DnsSeederDomain => Try[Seq[InetAddress]] = defaultLookupFunction)

object DnsClientInput {
  def defaultLookupFunction(url: DnsSeederDomain): Try[Seq[InetAddress]] = {
    Try(InetAddress.getAllByName(url.domainName))
  }
}