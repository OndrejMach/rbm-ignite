package com.tmobile.sit.rbm.config

import com.tmobile.sit.rbm.pipeline.Logger


/**
 * class which can be used as a helper for case classes holding configuration.
 * It provides printing methods not to bother yourself with implementing it and also it pushes coders to implement verification method 'isAllDefined'
 * to do at least simple checks of validity.
 *
 * @author Ondrej Machacek
 */
abstract class GenericSettings extends Logger {
  /**
   * prints fields fields which are missing (not defined). It works with any case classes inheriting from this class.
   */
  def printMissingFields(): Unit = {
    val fields = this.getClass.getDeclaredFields
    for (field <- fields) {
      field.setAccessible(true)
      val name = field.getName
      if (field.get(this).asInstanceOf[Option[String]].get.isEmpty)
        logger.info(s"${Console.RED}Missing entry: $name${Console.RESET}")
    }
  }

  /**
   * Prints all parameters and their values. Good for having a picture on what configuration was used for a particular job run.
   */
  def printAllFields(): Unit = {
    val fields = this.getClass.getDeclaredFields
    for (field <- fields) {
      field.setAccessible(true)
      logger.info(s"${Console.RED}${Console.BOLD}Parameter ${field.getName}:${Console.RESET} ${field.get(this).asInstanceOf[Option[String]].get}")
    }
  }

  /**
   * Abstract method pushing coders to verify all parameters are set properly.
   */
  def isAllDefined: Boolean
}