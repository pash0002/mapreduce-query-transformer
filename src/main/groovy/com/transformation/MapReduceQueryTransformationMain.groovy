package com.transformation

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The utility responsible extracting the mapreduce job related metadata from mapreduce job history done dir files
 */
@CompileStatic
@Slf4j
class MapReduceQueryTransformationMain {

  static void main(String[] args) {
    if (args.size() != 2) {
      throw new RuntimeException('Input and output directory should be pass')
    }
    TransformationSetup setup = new TransformationSetup(args[0], args[1])
    MapReduceQueryTransformer transformer = new MapReduceQueryTransformer(setup)
    transformer.transform()
    log.info('Finished')
  }

}
