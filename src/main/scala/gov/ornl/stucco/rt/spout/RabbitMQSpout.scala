package gov.ornl.stucco.rt.spout

import scala.util.control.Exception._

import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.spout.{SpoutOutputCollector, Scheme}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.{Fields, Values}

import com.rabbitmq.client.{
  ConnectionFactory,
  Connection,
  Channel,
  QueueingConsumer,
  ShutdownSignalException
}

import java.util.{Map => JMap}
import java.lang.{Long => JLong}

case class Queue(
  name: String,
  durable: Boolean = false,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  arguments: Option[JMap[String, AnyRef]] = None)

class RabbitMQSpout(
    queue: Queue,
    serializationScheme: Scheme,
    host: String,
    port: Int,
    username: String,
    password: String,
    virtualHost: String,
    requeueOnFail: Boolean)
  extends BaseRichSpout {

  val ERROR_STREAM_NAME = "error-stream"
  val DELAY_AFTER_SHUTDOWN: JLong = 1000 // milliseconds
  val DELIVERY_WAIT_TIME: JLong = 1 // milliseconds
  @transient private var collector: SpoutOutputCollector = _
  @transient private var connection: Connection = _
  @transient private var channel: Channel = _
  @transient private var consumer: QueueingConsumer = _
  @transient private var consumerTag: String = _

  override def open(
      config: JMap[_, _],
      context: TopologyContext,
      collector: SpoutOutputCollector) {
    this.collector = collector
    setupChannel()
  }

  override def close() {
    if (channel != null) {
      if (consumerTag != null) {
        channel.basicCancel(consumerTag)
      }
      channel.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def nextTuple() {
    try {
      if (consumer != null) {
        Option(consumer.nextDelivery(DELIVERY_WAIT_TIME)) foreach { delivery =>
          val tag: JLong = delivery.getEnvelope.getDeliveryTag
          val msg = delivery.getBody
          Option(serializationScheme deserialize msg) match {
            case None =>
              handleMalformed(tag, msg)
            case Some(deserialized) =>
              if (collector != null) collector.emit(deserialized, tag)
          }
        }
      }
    } catch {
      case e: ShutdownSignalException => { 
        sleep(DELAY_AFTER_SHUTDOWN)
        reconnect()
      }
    }
  }

  private def handleMalformed(tag: JLong, msg: Array[Byte]) {
    ack(tag) // to avoid retry loops
    if (collector != null) collector.emit(ERROR_STREAM_NAME, new Values(tag, msg))
  }

  override def ack(msgId: AnyRef) {
    msgId match {
      case tag: JLong => {
        if (channel != null) channel.basicAck(tag, false)
      }
      case _ => throw new RuntimeException("can't ack " + msgId + ": " +
        msgId.getClass.getName)
    }
  }

  override def fail(msgId: AnyRef) {
    msgId match {
      case tag: JLong => {
        if (channel != null) channel.basicReject(tag, requeueOnFail)
      }
      case _ => throw new RuntimeException("can't fail " + msgId + ": "
        + msgId.getClass.getName)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(serializationScheme.getOutputFields)
    declarer.declareStream(ERROR_STREAM_NAME, new Fields("deliveryTag", "bytes"))
  }

  private def setupChannel() {
    val cf = new ConnectionFactory
    cf.setHost(host)
    cf.setPort(port)
    cf.setUsername(username)
    cf.setPassword(password)
    cf.setVirtualHost(virtualHost)

    connection = cf.newConnection()
    channel = connection.createChannel()
    consumer = new QueueingConsumer(channel)

    val q = channel queueDeclare (queue.name, queue.durable, queue.exclusive,
      queue.autoDelete, queue.arguments.orNull)
    consumerTag = channel.basicConsume(q.getQueue, false, consumer)
  }

  private def reconnect() {
    setupChannel()
  }

  private def sleep(milliseconds: JLong) {
    ignoring(classOf[InterruptedException]) {
      Thread.sleep(milliseconds)
    }
  }
}

object RabbitMQSpout {
  
  val DEFAULT_HOST = "localhost"
  val DEFAULT_PORT = 5672
  val DEFAULT_USERNAME = "guest"
  val DEFAULT_PASSWORD = "guest"
  val DEFAULT_VIRTUALHOST = "/"
  val DEFAULT_REQUEUEONFAILPOLICY = true

  def apply(
      queue: Queue,
      serializationScheme: Scheme,
      host: String = DEFAULT_HOST,
      port: Int = DEFAULT_PORT,
      username: String = DEFAULT_USERNAME,
      password: String = DEFAULT_PASSWORD,
      virtualHost: String = DEFAULT_VIRTUALHOST,
      requeueOnFail: Boolean = DEFAULT_REQUEUEONFAILPOLICY) =
    new RabbitMQSpout(queue, serializationScheme, host, port, username,
      password, virtualHost, requeueOnFail)
}
