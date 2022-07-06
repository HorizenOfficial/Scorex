package scorex.core.network.dns

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scorex.core.network.dns.model.DnsClientInput
import scorex.core.network.dns.strategy.LookupStrategy
import scorex.util.ScorexLogging

import scala.util.{Failure, Success}

class DnsClient(dnsClientParams: DnsClientInput) extends Actor with ScorexLogging {

  import scorex.core.network.dns.DnsClient.ReceivableMessages.LookupRequest

  override def receive: Receive =
    dnsLookup orElse nonsense

  private def dnsLookup: Receive = {
    case LookupRequest(strategy: LookupStrategy) =>
      strategy.apply(dnsClientParams) match {
        case Success(response) => sender() ! response
        case Failure(exception) => log.error("Failed to perform a dns lookup: " + exception.getMessage)
      }
  }

  private def nonsense: Receive = {
    case nonsense: Any =>
      log.warn(s"DnsClient: got unexpected input $nonsense")
  }
}

object DnsClient {
  object ReceivableMessages {
    case class LookupRequest(s: LookupStrategy)
  }

  object Exception {
    final case class DnsLookupException(private val message: String = "")
      extends Exception(message)
  }
}

object DnsClientRef {
  def props(params: DnsClientInput): Props = {
    Props(new DnsClient(params))
  }

  def apply(dnsClientParams: DnsClientInput)(implicit system: ActorSystem): ActorRef = {
    system.actorOf(props(dnsClientParams))
  }
}