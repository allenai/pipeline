package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

// scalastyle:off magic.number
class TestVersionId extends UnitSpec {
  "Version ID" should "parse from string" in {
    MavenVersionId("1") should equal(Some(MavenVersionId(1)))
    MavenVersionId("1.2") should equal(Some(MavenVersionId(1, Some(2))))
    MavenVersionId("1.4.2") should equal(Some(MavenVersionId(1, Some(4), Some(2))))
    MavenVersionId("1.4.2-1") should equal(Some(MavenVersionId(1, Some(4), Some(2), Some(1))))
    MavenVersionId("1.4.2-1-SNAPSHOT") should equal(
      Some(MavenVersionId(1, Some(4), Some(2), Some(1), Some("SNAPSHOT")))
    )
    MavenVersionId("1.4.2_1") should equal(Some(MavenVersionId(1, Some(4), Some(2), Some(1))))
    MavenVersionId("1.4.2-beta-1") should equal(
      Some(MavenVersionId(1, Some(4), Some(2), Some(1), Some("beta")))
    )

    MavenVersionId(1, Some(2)) compareTo (MavenVersionId(1, Some(3))) should equal(-1)
    MavenVersionId(2, Some(3), None, Some(2)) compareTo (MavenVersionId(2, Some(3), None,
      Some(1))) should equal(1)
    MavenVersionId(2, Some(4)) compareTo (MavenVersionId(2, Some(4), Some(0))) should equal(-1)
  }

}
