package com.transformation


import com.transformation.parser.Parser
import groovy.transform.CompileStatic

/**
 * Pre-Processing class
 */
@CompileStatic
class MapReduceQueryTransformer {

  private final TransformationSetup setup
  private final List<File> files = [] as ArrayList
  private final Parser parser

  MapReduceQueryTransformer(TransformationSetup setup) {
    this.setup = setup
    parser = new Parser(setup.outputPath)
  }

  void transform() {
    getFilesRecursively(setup.inputPath)
    parser.parse(files)
  }

  void getFilesRecursively(File folder) {
    for (final File fileEntry : folder.listFiles()) {
      if (fileEntry.directory) {
        getFilesRecursively(fileEntry)
      } else {
        files.add(fileEntry)
      }
    }
  }

}
