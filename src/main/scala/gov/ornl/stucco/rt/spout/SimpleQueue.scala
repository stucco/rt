package gov.ornl.stucco.rt.spout

import java.util.{Map => JMap}

import com.rabbitmq.client.Channel

import com.rapportive.storm.amqp.QueueDeclaration

case class SimpleQueue(
    name: String,
    durable: Boolean = false,
    exclusive: Boolean = false,
    autoDelete: Boolean = false,
    arguments: JMap[String, AnyRef] = null) extends QueueDeclaration {

  override def isParallelConsumable() = true

  override def declare(channel: Channel) =
    channel.queueDeclare(name, durable, exclusive, autoDelete, arguments)
}
