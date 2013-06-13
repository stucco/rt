package gov.ornl.stucco.spout

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
  durable: Boolean,
  exclusive: Boolean,
  autoDelete: Boolean,
  arguments: Option[JMap[String, AnyRef]])

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
  @transient private var collector: Option[SpoutOutputCollector] = None
  @transient private var connection: Option[Connection] = None
  @transient private var channel: Option[Channel] = None
  @transient private var consumer: Option[QueueingConsumer] = None
  @transient private var consumerTag: Option[String] = None

  override def open(
      config: JMap[_, _],
      context: TopologyContext,
      collector: SpoutOutputCollector) {
    this.collector = Option(collector)
    setupChannel()
  }

  override def close() {
    channel foreach { c =>
      consumerTag foreach { c.basicCancel(_) }
      c.close()
    }
    connection foreach { _.close() }
  }

  override def nextTuple() {
    try {
      consumer foreach { c =>
        val delivery = c.nextDelivery()
        val tag: JLong = delivery.getEnvelope.getDeliveryTag
        val msg = delivery.getBody
        Option(serializationScheme deserialize msg) match {
          case None =>
            handleMalformed(tag, msg)
          case Some(deserialized) =>
            collector foreach { _.emit(deserialized, tag) }
        }
      }
    } catch {
      case e: ShutdownSignalException => { reconnect() }
    }
  }

  private def handleMalformed(tag: JLong, msg: Array[Byte]) {
    ack(tag) // to avoid retry loops
  }

  override def ack(msgId: AnyRef) {
    msgId match {
      case tag: JLong => {
        channel foreach { _.basicAck(tag, false) }
      }
      case _ => throw new RuntimeException("can't ack " + msgId + ": " +
        msgId.getClass.getName)
    }
  }

  override def fail(msgId: AnyRef) {
    msgId match {
      case tag: JLong => {
        channel foreach { _.basicReject(tag, requeueOnFail) }
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

    val conn = cf.newConnection()
    val chan = conn.createChannel()
    val cons = new QueueingConsumer(chan)

    val q = chan.queueDeclare(queue.name, queue.durable, queue.exclusive,
      queue.autoDelete, queue.arguments.orNull)
    val tag = chan.basicConsume(q.getQueue, false, cons)

    connection = Option(conn)
    channel = Option(chan)
    consumer = Option(cons)
    consumerTag = Option(tag)
  }

  private def reconnect() {
    setupChannel()
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
