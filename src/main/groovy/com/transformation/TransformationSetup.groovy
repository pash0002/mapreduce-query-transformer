package com.transformation

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Pre-Processing class
 */
@CompileStatic
@Slf4j
final class TransformationSetup  {

  private final File inputPath
  private final File outputPath

  TransformationSetup(String inputPath, String outputPath) {
    this.inputPath = new File(inputPath)
    this.outputPath = createOutputDirectory(outputPath)
  }

  File getInputPath() {
    inputPath
  }

  File getOutputPath() {
    outputPath
  }

  private static File createOutputDirectory(String outputPath) {
    File outputDirectoryPath = new File(outputPath)
    if (outputDirectoryPath.exists()) {
      throw new RuntimeException("The directory at path - $outputDirectoryPath.path already exists")
    }
    outputDirectoryPath.mkdirs()
    log.info("Output directory created at path - ${outputDirectoryPath.path}")
    outputDirectoryPath
  }

}
