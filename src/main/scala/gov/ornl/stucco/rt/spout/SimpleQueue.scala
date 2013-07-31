package gov.ornl.stucco.rt.spout

import java.util.{ Map => JMap }

import com.rabbitmq.client.Channel
import com.rabbitmq.client.AMQP.Queue.DeclareOk

import com.rapportive.storm.amqp.QueueDeclaration

import grizzled.slf4j.Logging

/**
 * A container to help declare a AMQP queue.
 *
 * @constructor Create a declaration of a named queue with the specified options.
 *
 * @param name The unique name of the queue.
 *
 * @param durable Whether the queue should remain active when the server
 * restarts. Messages sent to a durable queue can survive server restarts if
 * the messages are marked persistent.
 *
 * @param exclusive Whether the queue should only be able to be accessed
 * through the current connection. If a queue is marked exclusive, it will
 * be deleted when the connection closes.
 *
 * @param autoDelete Whether the queue should be automatically deleted
 * when all consumers have finished using it.
 *
 * @param arguments A set of arguments for the queue declaration. The syntax
 * and semantics of arguments depends on the server implementation.
 */
case class SimpleQueue(
  name: String,
  durable: Boolean = false,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  arguments: Option[JMap[String, AnyRef]] = None)
    extends QueueDeclaration
    with Logging {

  /**
   * Returns `true` as this queue is safe for parallel consumers
   */
  override def isParallelConsumable() = true

  /**
   * Declares a queue and creates the named queue if it does not exist.
   *
   * @param channel An open AMQP channel which can be used to send declarations.
   *
   * @return The server's response to the successful queue declaration.
   */
  override def declare(channel: Channel): DeclareOk =
    channel.queueDeclare(name, durable, exclusive, autoDelete, arguments.orNull)
}
