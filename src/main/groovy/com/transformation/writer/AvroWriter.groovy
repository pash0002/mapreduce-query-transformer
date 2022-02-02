package com.transformation.writer

import com.transformation.parser.JobHistoryBuilder
import com.transformation.parser.XmlConfigBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter

import static com.transformation.constant.WriterConstant.CPU_TIME
import static com.transformation.constant.WriterConstant.FINISH_TIME
import static com.transformation.constant.WriterConstant.JOB_ID
import static com.transformation.constant.WriterConstant.SCHEMA
import static com.transformation.constant.WriterConstant.SESSION_ID
import static com.transformation.constant.WriterConstant.START_TIME
import static com.transformation.constant.WriterConstant.USER_NAME
import static com.transformation.constant.WriterConstant.WORKFLOW_NAME
import static com.transformation.constant.WriterConstant.XML_QUERY

/**
 * Avro Serializer
 */
@CompileStatic
@Slf4j
class AvroWriter {

  private final OutputStream jobHistOutStream
  private final OutputStream xmlConfigOutStream
  private DataFileWriter<GenericRecord> jobHistWriter
  private DataFileWriter<GenericRecord> jobConfigWriter
  private final Schema jobHistSchema
  private final Schema jobConfigSchema

  AvroWriter(File outputPath) {
    jobHistOutStream = new FileOutputStream(new File(outputPath.path + File.separator + 'job_history.avro'))
    xmlConfigOutStream = new FileOutputStream(new File(outputPath.path + File.separator + 'xml_config.avro'))
    jobHistSchema = getSchema(getClass().getResourceAsStream('/schema-files/job_history.avsc'))
    jobConfigSchema = getSchema(getClass().getResourceAsStream('/schema-files/job_config.avsc'))
  }

  private static Schema getSchema(InputStream inputStream) {
    new Schema.Parser().parse(inputStream)
  }

  void initialize() {
    DatumWriter<GenericRecord> jobHistDatumWriter = new GenericDatumWriter<>(jobHistSchema)
    jobHistWriter = new DataFileWriter<>(jobHistDatumWriter)
    jobHistWriter.create(jobHistSchema, jobHistOutStream)
    DatumWriter<GenericRecord> jobConfigDatumWriter = new GenericDatumWriter<>(jobConfigSchema)
    jobConfigWriter = new DataFileWriter<>(jobConfigDatumWriter)
    jobConfigWriter.create(jobConfigSchema, xmlConfigOutStream)
    log.info('Initialized Job History and Xml Config writer')
  }

  void serialize(JobHistoryBuilder builder) {
    GenericRecord record = new GenericData.Record(jobHistSchema)
    record.put(JOB_ID, builder.jobId)
    record.put(USER_NAME, builder.userName)
    record.put(WORKFLOW_NAME, builder.workflowName)
    record.put(FINISH_TIME, builder.finishTime)
    record.put(CPU_TIME, builder.cpuTime)
    record.put(START_TIME, builder.startTime)
    jobHistWriter.append(record)
    jobHistWriter.flush()
  }

  void serialize(XmlConfigBuilder builder) {
    GenericRecord record = new GenericData.Record(jobConfigSchema)
    record.put(SCHEMA, builder.schema)
    record.put(JOB_ID, builder.jobId)
    record.put(SESSION_ID, builder.sessionId)
    record.put(XML_QUERY, builder.xmlQuery)
    jobConfigWriter.append(record)
    jobConfigWriter.flush()
  }

}
