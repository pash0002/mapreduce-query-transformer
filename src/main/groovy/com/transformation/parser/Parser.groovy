package com.transformation.parser


import com.transformation.parser.dto.AMStartedDto
import com.transformation.parser.dto.JobFinishedDto
import com.transformation.parser.dto.JobSubmittedDto
import com.transformation.writer.AvroWriter
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import groovy.util.logging.Slf4j
import org.apache.hadoop.mapreduce.jobhistory.AMStarted
import org.apache.hadoop.mapreduce.jobhistory.EventType
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent
import org.apache.hadoop.mapreduce.jobhistory.JobFinished
import org.apache.hadoop.mapreduce.jobhistory.JobSubmitted
import org.apache.hadoop.tools.rumen.JobConfigurationParser
import org.apache.hadoop.tools.rumen.JobHistoryParser
import org.apache.hadoop.tools.rumen.JobHistoryParserFactory
import org.apache.hadoop.tools.rumen.RewindableInputStream

import static com.transformation.constant.ParserConstant.CONF
import static com.transformation.constant.ParserConstant.HIVE_CURRENT_DB
import static com.transformation.constant.ParserConstant.HIVE_QUERY_STRING
import static com.transformation.constant.ParserConstant.HIVE_SESSION_ID
import static com.transformation.constant.ParserConstant.JHIST
import static com.transformation.constant.ParserConstant.XML

/**
 * Parser class to parse job history and config files
 */
@CompileStatic
@Slf4j
class Parser {

  private final File outputPath
  private final AvroWriter writer
  private Properties properties
  private RewindableInputStream ris
  private JobHistoryParser parser

  Parser(File outputPath) {
    this.outputPath = outputPath
    writer = new AvroWriter(outputPath)
  }

  void parse(List<File> files) {
    writer.initialize()
    log.info("Started the parsing job history and config files with count - ${files.size()}")
    files.collect { File entry ->
      String fileName = entry.name
      if (fileName.endsWith(JHIST)) {
        processJobHistoryFile(entry)
      } else if (fileName.endsWith(XML)) {
        processConfigfile(entry)
      } else {
        log.error("Unknown file format for a file $fileName")
      }
    }
    log.info('Parsed Successfully')
  }

  private void processJobHistoryFile(File file) {
    JobHistoryBuilder builder = parseJobHistoryFile(file)
    log.debug('Serializing the Job History Data')
    writer.serialize(builder)
  }

  private JobHistoryBuilder parseJobHistoryFile(File file) {
    ris = new RewindableInputStream(new FileInputStream(file))
    parser = JobHistoryParserFactory.getParser(ris)
    AMStartedDto amStartedDto = null
    JobFinishedDto jobFinishedDto = null
    JobSubmittedDto jobSubmittedDto = null
    HistoryEvent historyEvent = parser.nextEvent()
    while (historyEvent != null) {
      switch (historyEvent.getEventType()) {
        case EventType.AM_STARTED:
          amStartedDto = AMStartedDto.create(((AMStarted) historyEvent.datum))
          break
        case EventType.JOB_SUBMITTED:
          jobSubmittedDto = JobSubmittedDto.create(((JobSubmitted) historyEvent.datum))
          break
        case EventType.JOB_FINISHED:
          jobFinishedDto = JobFinishedDto.create((JobFinished) historyEvent.datum)
          break
      }
      historyEvent = parser.nextEvent()
    }
    new JobHistoryBuilder.Builder(jobSubmittedDto.jobId)
        .withUserName(jobSubmittedDto.userName)
        .withWorkflowName(jobSubmittedDto.workflowName)
        .withFinishTime(jobFinishedDto ? jobFinishedDto.finishTime : 0)
        .withCpuTime(jobFinishedDto ? jobFinishedDto.cpuTime : 0)
        .withStartTime(amStartedDto.startTime).build()
  }

  private void processConfigfile(File file) {
    XmlConfigBuilder builder = parseXmlConfigFile(file)
    log.debug('Serializing the Job Config Data')
    writer.serialize(builder)
  }

  @TypeChecked(TypeCheckingMode.SKIP)
  private XmlConfigBuilder parseXmlConfigFile(File file) {
    properties = JobConfigurationParser.parse(new FileInputStream(file))
    String fileName = file.name
    String jobId = fileName.split(CONF)[0]
    new XmlConfigBuilder(getValue(HIVE_CURRENT_DB), jobId, getValue(HIVE_SESSION_ID), getValue(HIVE_QUERY_STRING))
  }

  private String getValue(String property) {
    properties.getProperty(property)
  }

}

