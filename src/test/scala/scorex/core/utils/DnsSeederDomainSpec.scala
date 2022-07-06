package scorex.core.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import scorex.core.network.dns.model.DnsSeederDomain

class DnsSeederDomainSpec extends AnyFlatSpec {
  "A DnsSeederDomain" should "be initialized correctly" in {
    // Arrange
    val correctDomainNameSeq = Seq("domain-test.com", "domain.sub-domain.global", "this_is_a_domain.test.dom.horizen", "a.b.c.d.e.f.g.uk.co")

    // Act
    val webNameSeq = correctDomainNameSeq.map(domain => new DnsSeederDomain(domain))

    // Assert
    for ((domainString, webName) <- correctDomainNameSeq zip webNameSeq) {
      webName.domainName should be(domainString)
    }
  }

  "A DnsSeederDomain" should "throw and IllegalArgumentException" in {
    // Arrange
    val correctDomainNameSeq = Seq("domain-test.com.", "", "     ", ".bad-web-domain.com", "test,domain.global")
    val createDnsSeederDomain = (domain: String) => new DnsSeederDomain(domain)

    // Act & Assert
    correctDomainNameSeq.foreach(domain => assertThrows[IllegalArgumentException] {
      createDnsSeederDomain(domain)
    })
  }
}
