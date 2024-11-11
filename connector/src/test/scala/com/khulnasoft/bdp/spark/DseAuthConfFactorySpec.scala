/*
 * Copyright KhulnaSoft, Inc.
 *
 * Please see the included license file for details.
 */

package com.khulnasoft.bdp.spark
/**
TODO:
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import com.khulnasoft.bdp.config.{ClientConfigurationFactory, YamlClientConfiguration}
import com.khulnasoft.bdp.spark.DseAuthConfFactory.DsePasswordAuthConf
import com.khulnasoft.bdp.test.ng.{DataGenerator, DseScalaTestBase, ToString, YamlProvider}

class DseAuthConfFactorySpec extends FlatSpec with Matchers with DseScalaTestBase {
  
  beforeClass {
    YamlProvider.provideDefaultYamls()
    YamlClientConfiguration.setAsClientConfigurationImpl()
  }
  
  it should "produce equivalent AuthConf instances for the same SparkConf" in {
    def genAuthConf = DseAuthConfFactory.authConf(new SparkConf())

    genAuthConf shouldBe genAuthConf
  }

  it should "produce comparable DsePasswordAuthConf instances" in {
    val gen = new DataGenerator()
    val cases = gen.generate[DsePasswordAuthConf]()
    for (c <- cases) {
      withClue(s"Comparing ${ToString.toStringWithNames(c)} failed") {
        val duplicate = SerializationUtils.roundtrip(c)
        duplicate shouldBe c
      }
    }
  }
}
  **/
