package scorex.core.network.dns.model

class DnsSeederDomain(val domainName: String) {
  private val WEB_DOMAIN_REGEX = "^(((?!\\-))(xn\\-\\-)?[a-z0-9\\-_]{0,61}[a-z0-9]{1,1}\\.)*(xn\\-\\-)?([a-z0-9\\-]{1,61}|[a-z0-9\\-]{1,30})\\.[a-z]{2,}$".r

  domainName match {
    case e if e.trim.isEmpty || WEB_DOMAIN_REGEX.findFirstMatchIn(e).isEmpty => throw new IllegalArgumentException("Domain name is not well formed")
    case _ =>
  }
}