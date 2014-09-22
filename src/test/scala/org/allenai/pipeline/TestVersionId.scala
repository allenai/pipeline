package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

/**
 * Created by rodneykinney on 9/19/14.
 */
class TestVersionId extends UnitSpec {
  "Version ID" should "parse from string" in {
    MavenVersionId("1") should equal(Some(MavenVersionId(1)))
    MavenVersionId("1.2") should equal(Some(MavenVersionId(1, Some(2))))
    MavenVersionId("1.4.2") should equal(Some(MavenVersionId(1, Some(4), Some(2))))
    MavenVersionId("1.4.2-1") should equal(Some(MavenVersionId(1, Some(4), Some(2), Some(1))))
    MavenVersionId("1.4.2-1-SNAPSHOT") should equal(
      Some(MavenVersionId(1, Some(4), Some(2), Some(1), Some("SNAPSHOT"))))
    MavenVersionId("1.4.2_1") should equal(Some(MavenVersionId(1, Some(4), Some(2), Some(1))))
    MavenVersionId("1.4.2-beta-1") should equal(
      Some(MavenVersionId(1, Some(4), Some(2), Some(1), Some("beta"))))

    MavenVersionId(1,Some(2)) compareTo(MavenVersionId(1,Some(3))) should equal(-1)
    MavenVersionId(2,Some(3), None, Some(2)) compareTo(MavenVersionId(2, Some(3), None,
      Some(1))) should equal(1)
    MavenVersionId(2, Some(4)) compareTo(MavenVersionId(2, Some(4), Some(0))) should equal(-1)
  }

  "Version History" should "determine unchangedSince" in {
    val obj1 = new UnknownCodeInfo {}
    val info1 = obj1.codeInfo
    info1.unchangedSince should equal(info1.buildId)

    val obj2 = new Ai2CodeInfo {}
    val info2 = obj2.codeInfo
    info2.unchangedSince should equal(info2.buildId)

    val obj3 = new Ai2CodeInfo {
      override def versionHistory = List("1.1.0", "1.2.0-1", "1.6")
    }
    obj3.lastPrecedingChangeId("1.1.0") should equal("1.1.0")
    obj3.lastPrecedingChangeId("1.2.0") should equal("1.1.0")
    obj3.lastPrecedingChangeId("1.2.0-beta-1") should equal("1.2.0-1")
    obj3.lastPrecedingChangeId("1.2.8") should equal("1.2.0-1")
    obj3.lastPrecedingChangeId("1.4.0") should equal("1.2.0-1")
    obj3.lastPrecedingChangeId("1.7.0") should equal("1.6")
    obj3.lastPrecedingChangeId("1.6-SNAPSHOT") should equal("1.6")
  }

}
