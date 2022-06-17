package com.tmobile.sit.rbm.pipeline


/**
 * Writer interface for basically any writer class implemented here.
 */
trait Writer extends Logger {
  def writeData() : Unit
}
