package com.transformation.constant

import groovy.transform.CompileStatic

/**
 * Class that holds the constants
 */
@CompileStatic
final class ParserConstant {

  final static String TASK_CNT = 'org.apache.hadoop.mapreduce.TaskCounter'
  final static String CPU_MILLISECONDS = 'CPU_MILLISECONDS'
  final static String CONF = '_conf'
  final static String HIVE_CURRENT_DB = 'hive.current.database'
  final static String HIVE_QUERY_STRING = 'hive.query.string'
  final static String HIVE_SESSION_ID = 'hive.session.id'
  final static String JHIST = '.jhist'
  final static String XML = '.xml'

}
