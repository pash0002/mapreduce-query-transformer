package com.transformation.parser.dto

import groovy.transform.CompileStatic
import org.apache.hadoop.mapreduce.jobhistory.AMStarted

/**
 * Hold the Job related metadata
 */
@CompileStatic
class AMStartedDto {

  private final Long startTime

  private AMStartedDto(Long startTime) {
    this.startTime = startTime
  }

  static AMStartedDto create(AMStarted amStarted) {
    new AMStartedDto(amStarted.startTime)
  }

  Long getStartTime() {
    return startTime
  }

}
