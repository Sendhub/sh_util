"""Unit tests for ``sh_util.text.ec2HostnameToIp.ec2_hostname_to_ip``.

Replaces an embedded EC2 public-DNS hostname (``ec2-A-B-C-D.compute-N.amazonaws.com``)
with its dashed IP segments turned back into a dotted IP. Strings with no EC2
hostname are returned unchanged.
"""

from sh_util.text.ec2HostnameToIp import ec2_hostname_to_ip


class TestEc2HostnameToIp:
    def test_hostname_embedded_in_a_connection_string_is_replaced(self):
        s = "postgres://*:*@ec2-107-22-243-182.compute-1.amazonaws.com:5432/dbname"
        assert ec2_hostname_to_ip(s) == "postgres://*:*@107.22.243.182:5432/dbname"

    def test_bare_hostname_is_replaced(self):
        s = "ec2-1-2-3-4.compute-1.amazonaws.com"
        assert ec2_hostname_to_ip(s) == "1.2.3.4"

    def test_string_without_an_ec2_hostname_is_unchanged(self):
        s = "This should come back unchanged"
        assert ec2_hostname_to_ip(s) == s

    def test_empty_string_is_unchanged(self):
        assert ec2_hostname_to_ip("") == ""

    def test_different_compute_region_digit_is_matched(self):
        s = "ec2-8-8-8-8.compute-2.amazonaws.com"
        assert ec2_hostname_to_ip(s) == "8.8.8.8"
