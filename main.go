package main

import (
	"flag"

	"github.com/smbrave/mysql-kafka/tunnel"
)

var (
	flag_mysql_addr  = flag.String("mysql_addr", "127.0.0.1:3306", "mysql addr")
	flag_mysql_user  = flag.String("mysql_user", "root", "msyql user")
	flag_mysql_pass  = flag.String("mysql_pass", "root", "mysql password")
	flag_kafka_topic = flag.String("kafka_topic", "test", "kafka topic")
	flag_kafka_addr  = flag.String("kafka_addr", "127.0.0.1:9092", "kafka addr split by ',' ")
)

func main() {
	flag.Parse()

	cfg := tunnel.NewConfig()
	cfg.MysqlAddr = *flag_mysql_addr
	cfg.MysqlUser = *flag_mysql_user
	cfg.MysqlPass = *flag_mysql_pass
	cfg.KafkaTopic = *flag_kafka_topic
	cfg.KafkaAddr = *flag_kafka_addr

	tunel := tunnel.NewTunnel(cfg)
	err := tunel.Start()
	if err != nil {
		panic(err)
	}
	select {}
	tunel.Stop()
}
