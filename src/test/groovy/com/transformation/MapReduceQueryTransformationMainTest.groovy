package com.transformation

import spock.lang.Specification

/**
 * Test class for utility
 */
class MapReduceQueryTransformationMainTest extends Specification {

  private final String inputPath = new File('.').canonicalPath + '/src/test/resources/job-files'
  private final String outputPath = new File('./build/temp_output')

  void setup() {
    File file = new File(outputPath)
    if (file.exists() && file.directory) {
      file.deleteDir()
    }
  }

  def "Test the transformation utility"() {
    when:
    MapReduceQueryTransformer transformer =
        new MapReduceQueryTransformer(new TransformationSetup(inputPath, outputPath))
    transformer.transform()
    then:
    File file = new File(outputPath)
    assert file.directory
    assert file.listFiles().size() == 2
  }

}
