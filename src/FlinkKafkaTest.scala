import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkKafkaTest {

  private val ZOOKEEPER_HOST = "xx.xx.xx.xx:2181"
  private val KAFKA_BROKER = "xx.xx.xx.xx:9092"
  private val TRANSACTION_GROUP = "giant-data-group"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //topic的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction: DataStream[String] = env.addSource(
        new FlinkKafkaConsumer[String]("giant-user-behavior", new SimpleStringSchema(), kafkaProps)
    )

    transaction.print()

    env.execute()
  }
}
