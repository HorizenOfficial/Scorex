package sparkz.core.utils

import java.net.{Inet4Address, Inet6Address, InetSocketAddress}

class NetworkAddressWrapper(address: InetSocketAddress) {
  private val stringAddr = if (isIPv4) address.getHostString.split('.')
                           else address.getAddress.getHostAddress.split(':')

  def isLocal: Boolean = NetworkUtils.isLocalAddress(address.getAddress)
  def isRoutable: Boolean = {
    !(isRFC1918 || isRFC2544 || isRFC3927 || isRFC6598 || isRFC5737 || isLocal)
  }
  def isIPv4: Boolean = address.getAddress match {
    case _: Inet4Address => true
    case _ => false
  }
  def isIPv6: Boolean = address.getAddress match {
    case _: Inet6Address => true
    case _ => false
  }

  def hasLinkedIPv4: Boolean = {
    isRoutable && (isIPv4 || isRFC6145 || isRFC6052 || isRFC3964 || isRFC4380)
  }

  private def isBetween(str: String, lower: Int, higher: Int): Boolean = {
    val num = str.toInt
    num >= lower && num <= higher
  }

  // IPv4 private networks (10.0.0.0/8, 192.168.0.0/16, 172.16.0.0/12)
  def isRFC1918: Boolean = {
    isIPv4 && (
      stringAddr(0) == "10" ||
        (stringAddr(0) == "192" && stringAddr(1) == "168") ||
        (stringAddr(0) == "172" && isBetween(stringAddr(1), 16, 31))
      )
  }

  // IPv4 inter-network communications (192.18.0.0/15)
  def isRFC2544: Boolean = {
    isIPv4 && (
      stringAddr(0) == "198" && isBetween(stringAddr(1), 18, 19)
    )
  }

  // IPv4 ISP-level NAT (100.64.0.0/10)
  def isRFC6598: Boolean = {
    isIPv4 && (
      stringAddr(0) == "100" && isBetween(stringAddr(1), 64, 127)
    )
  }

  // IPv4 documentation addresses (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24)
  def isRFC5737: Boolean = {
    isIPv4 && (
      (stringAddr(0) == "192" && stringAddr(1) == "0" && stringAddr(2) == "2") ||
        (stringAddr(0) == "198" && stringAddr(1) == "51" && stringAddr(2) == "100") ||
        (stringAddr(0) == "203" && stringAddr(1) == "0" && stringAddr(2) == "113")
    )
  }

  // IPv4 autoconfig (169.254.0.0/16)
  def isRFC3927: Boolean = {
    isIPv4 && (stringAddr(0) == "169" && stringAddr(1) == "254")
  }

  // IPv6 documentation address (2001:0DB8::/32)
  def isRFC3849: Boolean = {
    isIPv6 && stringAddr(0) == "2001" && stringAddr(1) == "0db8"
  }

  // IPv6 6to4 tunnelling (2002::/16)
  def isRFC3964: Boolean = {
    isIPv6 && stringAddr(0) == "2002"
  }

  // IPv6 unique local (FC00::/7)
  def isRFC4193: Boolean = {
    isIPv6 && stringAddr(0) == "fc00"
  }

  // IPv6 Teredo tunnelling (2001::/32)
  def isRFC4380: Boolean = {
    isIPv6 && stringAddr(0) == "2001" && stringAddr(1) == "0"
  }

  // IPv6 ORCHID (2001:10::/28)
  def isRFC4843: Boolean = {
    isIPv6 && stringAddr(0) == "2001" && stringAddr(1) == "10"
  }

  // IPv6 autoconfig (FE80::/64)
  def isRFC4862: Boolean = {
    isIPv6 && stringAddr(0) == "fe80"
  }

  // IPv6 well-known prefix (64:FF9B::/96)
  def isRFC6052: Boolean = {
    isIPv6 && stringAddr(0) == "64" && stringAddr(1) == "ff9b"
  }

  // IPv6 IPv4-translated address (::FFFF:0:0:0/96)
  def isRFC6145: Boolean = {
    isIPv6 && stringAddr(3) == "ffff"
  }

  def isHeNet: Boolean = {
    isIPv6 && stringAddr(0) == "2001" && stringAddr(1) == "470"
  }

  def getSubNum(index: Int): Array[Byte] = {
    stringAddr(index).getBytes
  }
}

object NetworkAddressWrapper extends Enumeration {
  val NET_UNROUTABLE: Int = 0
  val NET_IPV4: Int = 1
  val NET_IPV6: Int = 2
}