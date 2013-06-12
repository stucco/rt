package gov.ornl.stucco.topology

import gov.ornl.stucco.spout.{RabbitMQSpout, Queue}

import backtype.storm.{Config, LocalCluster, StormSubmitter}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils

object Topology {
  val NUM_WORKERS = 3

  def main(args: Array[String]) {
    val builder = new TopologyBuilder

    // builder.setSpout("spoutname", new Thing, instances)
    // builder.setBolt("boltname", new OtherThing, instances)
    // .shuffleGrouping("spoutname")

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
}
