package gov.ornl.stucco.rt.topology

import gov.ornl.stucco.rt.spout._
import gov.ornl.stucco.rt.bolt._

import org.streum.configrity._
import org.streum.configrity.yaml._

import grizzled.slf4j.Logging

import backtype.storm.{Config, LocalCluster, StormSubmitter}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.spout.Scheme

import com.rapportive.storm.spout.AMQPSpout

/** An object to define and run the storm topology.
  *
  * This object defines the topology, and then it runs it either
  * locally or remotely.
  *
  * This object builds the storm topology using a `TopologyBuilder`.
  * Given an IP address for the Nimbus server, it submits the topology
  * using `StormSubmitter`. Otherwise, it runs the topology locally using
  * a `LocalCluster`.
  */
object Topology extends Logging {

  /** Location of the settings file.
    */
  private val settingsFile = "config.yaml"

  private val settings = Configuration.load("config.yaml", YAMLFormat)

  /** Create and run the storm topology.
    *
    * @param args Empty array or an array containing a single string - the
    * IP address of the Nimbus machine.
    */
  def main(args: Array[String]) {
    val builder = new TopologyBuilder
    buildTopology(builder)

    val config = new Config
    config.setDebug(settings[Boolean]("storm.debug"))

    if (args.nonEmpty) {
      config.setNumWorkers(settings[Int]("storm.workers"))
      StormSubmitter.submitTopology(args(0), config, builder.createTopology())
    } else {
      val cluster = new LocalCluster
      cluster.submitTopology("Topology", config, builder.createTopology())
      // a storm topology runs forever (until it's killed)
      while (true) {
        Utils.sleep(5000)
      }
      cluster.killTopology("Topology")
      cluster.shutdown()
    }
  }

  /** Build the topology.
    *
    * Builds the entire topology for the computation.
    *
    * @param builder The `TopologyBuilder` to use.
    *
    * @see `topology.txt` - a ASCII art graphical depiction of the topology
    */
  def buildTopology(builder: TopologyBuilder) {
    val spout = buildSpout(new Deserializer)
    // build spout
    builder.setSpout("rabbitmq", spout, settings[Int]("instances.rabbitmq"))
    builder.setBolt("uuid", new UUIDBolt, settings[Int]("instances.uuid"))
      .shuffleGrouping("rabbitmq")
    builder.setBolt("route", new RouteBolt, settings[Int]("instances.route"))
      .shuffleGrouping("uuid")
    // build structured bolts
    builder.setBolt("parse", new ParseBolt, settings[Int]("instances.parse"))
      .shuffleGrouping("route", "structured")
    builder.setBolt("split", new SplitBolt, settings[Int]("instances.split"))
      .shuffleGrouping("parse")
    builder.setBolt("structuredgraph", new StructuredGraphBolt, settings[Int]("instances.structuredgraph"))
      .shuffleGrouping("split")
    // build unstructured bolts
    builder.setBolt("extract", new ExtractBolt, settings[Int]("instances.extract"))
      .shuffleGrouping("route", "unstructured")
    builder.setBolt("concept", new ConceptBolt, settings[Int]("instances.concept"))
      .shuffleGrouping("extract")
    builder.setBolt("relation", new RelationBolt, settings[Int]("instances.relation"))
      .shuffleGrouping("concept")
    builder.setBolt("unstructuredgraph", new UnstructuredGraphBolt, settings[Int]("instances.unstructuredgraph"))
      .shuffleGrouping("relation")
    // build common bolts
    builder.setBolt("document", new DocumentBolt, settings[Int]("instances.document"))
      .shuffleGrouping("parse").shuffleGrouping("extract")
    builder.setBolt("graph", new GraphBolt, settings[Int]("instances.graph"))
      .shuffleGrouping("structuredgraph").shuffleGrouping("unstructuredgraph")
  }

  /** Build the spout.
    *
    * Constructs a RabbitMQ queue and using a given data deserialization scheme,
    * creates a RabbitMQ spout.
    *
    * @param scheme The scheme to use to deserialize binary data from the queue.
    */
  def buildSpout(scheme: Scheme) = {
    val name = settings[String]("rabbitmq.queue.name")
    val durable = settings[Boolean]("rabbitmq.queue.durable")
    val exclusive = settings[Boolean]("rabbitmq.queue.exclusive")
    val autoDelete = settings[Boolean]("rabbitmq.queue.autoDelete")

    val queue = SimpleQueue(name, durable, exclusive, autoDelete)

    val host = settings[String]("rabbitmq.host")
    val port = settings[Int]("rabbitmq.port")
    val username = settings[String]("rabbitmq.username")
    val password = settings[String]("rabbitmq.password")
    val vhost = settings[String]("rabbitmq.vhost")

    new AMQPSpout(host, port, username, password, vhost, queue, scheme)
  }
}
