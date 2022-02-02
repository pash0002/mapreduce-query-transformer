package com.transformation.parser

import groovy.transform.CompileStatic

/**
 * The class holds the Job History Metadata
 */
@CompileStatic
class JobHistoryBuilder {

  final private String jobId
  final private String userName
  final private String workflowName
  final private Long finishTime
  final private Double cpuTime
  final private Long startTime

  private JobHistoryBuilder(Builder builder) {
    this.jobId = builder.jobId
    this.userName = builder.userName
    this.workflowName = builder.workflowName
    this.finishTime = builder.finishTime
    this.cpuTime = builder.cpuTime
    this.startTime = builder.startTime
  }

  String getJobId() {
    jobId
  }

  String getUserName() {
    userName
  }

  String getWorkflowName() {
    workflowName
  }

  Long getFinishTime() {
    finishTime
  }

  Double getCpuTime() {
    cpuTime
  }

  Long getStartTime() {
    startTime
  }

  static class Builder {

    String jobId
    String userName
    String workflowName
    Long finishTime
    Double cpuTime
    Long startTime

    Builder(String jobId) {
      this.jobId = jobId
    }

    Builder withUserName(String userName) {
      this.userName = userName
      this
    }

    Builder withWorkflowName(String workflowName) {
      this.workflowName = workflowName
      this
    }

    Builder withFinishTime(Long finishTime) {
      this.finishTime = finishTime
      this
    }

    Builder withCpuTime(Double cpuTime) {
      this.cpuTime = cpuTime
      this
    }

    Builder withStartTime(Long startTime) {
      this.startTime = startTime
      this
    }

    JobHistoryBuilder build() {
      new JobHistoryBuilder(this)
    }

  }

}
