package gov.ornl.stucco.rt.topology

import gov.ornl.stucco.rt.spout.{SimpleQueue, Deserializer}

import com.typesafe.config._

import grizzled.slf4j.Logging

import backtype.storm.{Config, LocalCluster, StormSubmitter}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.spout.Scheme

import com.rapportive.storm.spout.AMQPSpout

object Topology extends Logging {
  val NUM_WORKERS = 3
  val settings = ConfigFactory.load() // application.conf

  def main(args: Array[String]) {
    val builder = new TopologyBuilder

    val config = new Config
    config.setDebug(true)

    if (args.nonEmpty) {
      config.setNumWorkers(NUM_WORKERS)
      StormSubmitter.submitTopology(args(0), config, builder.createTopology())
    } else {
      val cluster = new LocalCluster
      cluster.submitTopology("Topology", config, builder.createTopology())
      Utils.sleep(5000)
      cluster.killTopology("Topology")
      cluster.shutdown()
    }
  }

  def buildTopology(builder: TopologyBuilder) {
    val spout = buildSpout(new Deserializer)
    builder.setSpout("spout", spout, settings getInt "instances.spout")
    // builder.setBolt("boltname", new OtherThing, instances)
    // .shuffleGrouping("spoutname")
  }

  def buildSpout(scheme: Scheme) = {
    val name = settings getString "rabbitmq.queue.name"
    val durable = settings getBoolean "rabbitmq.queue.durable"
    val exclusive = settings getBoolean "rabbitmq.queue.exclusive"
    val autoDelete = settings getBoolean "rabbitmq.queue.autoDelete"

    val queue = SimpleQueue(name, durable, exclusive, autoDelete)

    val host = settings getString "rabbitmq.host"
    val port = settings getInt "rabbitmq.port"
    val username = settings getString "rabbitmq.username"
    val password = settings getString "rabbitmq.password"
    val vhost = settings getString "rabbitmq.vhost"

    new AMQPSpout(host, port, username, password, vhost, queue, scheme)
  }
}
