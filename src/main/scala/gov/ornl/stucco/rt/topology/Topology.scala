package gov.ornl.stucco.rt.topology

import gov.ornl.stucco.rt.spout._
import gov.ornl.stucco.rt.bolt._

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
    builder.setSpout("rabbitmq", spout, settings getInt "instances.rabbitmq")
    builder.setBolt("uuid", new UUIDBolt, settings getInt "instances.uuid")
      .shuffleGrouping("rabbitmq")
    builder.setBolt("route", new RouteBolt, settings getInt "instances.route")
      .shuffleGrouping("uuid")
    // structured
    builder.setBolt("parse", new ParseBolt, settings getInt "instances.parse")
      .shuffleGrouping("route", "structured")
    builder.setBolt("split", new SplitBolt, settings getInt "instances.split")
      .shuffleGrouping("parse")
    builder.setBolt("structuredgraph", new StructuredGraphBolt, settings getInt "instances.structuredgraph")
      .shuffleGrouping("split")
    // unstructured
    builder.setBolt("extract", new ExtractBolt, settings getInt "instances.extract")
      .shuffleGrouping("route", "unstructured")
    builder.setBolt("concept", new ConceptBolt, settings getInt "instances.concept")
      .shuffleGrouping("extract")
    builder.setBolt("relation", new RelationBolt, settings getInt "instances.relation")
      .shuffleGrouping("concept")
    builder.setBolt("unstructuredgraph", new UnstructuredGraphBolt, settings getInt "instances.unstructuredgraph")
      .shuffleGrouping("relation")
    // both (merging)
    builder.setBolt("graph", new GraphBolt, settings getInt "instances.graph")
      .shuffleGrouping("relation")
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
