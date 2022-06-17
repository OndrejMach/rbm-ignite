package com.tmobile.sit.rbm.pipeline

import org.slf4j.LoggerFactory


/**
 * Logger class providing slf4j based logger. Logger configuration can be provided in log4j.properties (for example in the resources folder).
 *
 */
trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
