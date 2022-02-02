package com.transformation.parser.dto

import groovy.transform.CompileStatic
import org.apache.hadoop.mapreduce.jobhistory.JhCounterGroup
import org.apache.hadoop.mapreduce.jobhistory.JobFinished

import static com.transformation.constant.ParserConstant.CPU_MILLISECONDS
import static com.transformation.constant.ParserConstant.TASK_CNT

/**
 * Hold the Job related metadata
 */
@CompileStatic
class JobFinishedDto {

  private final Long finishTime
  private final Double cpuTime

  private JobFinishedDto(Long finishTime, Double cpuTime) {
    this.finishTime = finishTime
    this.cpuTime = cpuTime
  }

  static JobFinishedDto create(JobFinished jobFinished) {
    new JobFinishedDto(jobFinished.finishTime, getCpuTime(jobFinished))
  }

  static Double getCpuTime(JobFinished jobFinished) {
    jobFinished.totalCounters.groups.find { JhCounterGroup jhCounterGroup ->
      jhCounterGroup.getName().toString() == TASK_CNT
    }?.counts?.find {
      it.getName().toString() == CPU_MILLISECONDS
    }?.value
  }

  Long getFinishTime() {
    return finishTime
  }

  Double getCpuTime() {
    return cpuTime
  }

}
