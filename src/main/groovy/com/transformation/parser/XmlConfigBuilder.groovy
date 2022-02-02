package com.transformation.parser

import groovy.transform.CompileStatic

/**
 * The class holds the Job History config Metadata
 */
@CompileStatic
class XmlConfigBuilder {

  private final String schema
  private final String jobId
  private final String sessionId
  private final String xmlQuery

  XmlConfigBuilder(String schema, String jobId, String sessionId, String xmlQuery) {
    this.schema = schema
    this.jobId = jobId
    this.sessionId = sessionId
    this.xmlQuery = xmlQuery
  }

  String getSchema() {
    schema
  }

  String getJobId() {
    jobId
  }

  String getSessionId() {
    sessionId
  }

  String getXmlQuery() {
    xmlQuery
  }

}
