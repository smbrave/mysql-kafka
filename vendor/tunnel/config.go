package tunnel

import (
	"math/rand"
	"time"
)

type Config struct {
	LogDir  string
	LogFile string

	MysqlAddr    string
	MysqlUser    string
	MysqlPass    string
	MysqlFilter  string
	MysqlSlaveId uint32

	KafkaTopic string
	KafkaAddr  string
}

func NewConfig() *Config {
	config := new(Config)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	config.MysqlSlaveId = r.Uint32()
	config.LogDir = "/tmp"
	config.LogFile = "mysql-kafka.log"
	return config
}

type TunelData struct {
	Database string `json:"database"` //数据库
	Table    string `json:"table"`    //数据库表
	Action   string `json:"action"`   //新增、删除、更新
	Key      string `json:"key"`      //主键（删除用）
	Data     string `json:"data"`     //更新数据
	OData    string `json:"odata"`    //老数据（更新用）
}
