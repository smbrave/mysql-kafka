package main

import (
	"tunnel"
)

func main() {
	cfg := tunnel.NewConfig()
	cfg.MysqlAddr = "10.95.116.81:3306"
	cfg.MysqlUser = "root"
	cfg.MysqlPass = "123456"
	cfg.KafkaTopic = "test"
	cfg.KafkaAddr = "10.95.136.254:9092"

	tunel := tunnel.NewTunnel(cfg)
	err := tunel.Start()
	if err != nil {
		panic(err)
	}
	select {}
	tunel.Stop()
}
