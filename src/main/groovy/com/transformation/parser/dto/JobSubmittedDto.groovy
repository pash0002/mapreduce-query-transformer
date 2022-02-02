package com.transformation.parser.dto

import groovy.transform.CompileStatic
import org.apache.hadoop.mapreduce.jobhistory.JobSubmitted

/**
 * Hold the Job related metadata
 */
@CompileStatic
class JobSubmittedDto {

  private final String jobId
  private final String userName
  private final String workflowName

  private JobSubmittedDto(CharSequence jobId, CharSequence userName, CharSequence workflowName) {
    this.jobId = (String) jobId
    this.userName = (String) userName
    this.workflowName = (String) workflowName
  }

  static JobSubmittedDto create(JobSubmitted jobSubmitted) {
    new JobSubmittedDto(jobSubmitted.jobid, jobSubmitted.userName, jobSubmitted.workflowName)
  }

  String getJobId() {
    return jobId
  }

  String getUserName() {
    return userName
  }

  String getWorkflowName() {
    return workflowName
  }

}
