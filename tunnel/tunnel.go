package tunnel

import (
	"github.com/siddontang/go-mysql/canal"
)

type Tunnel struct {
	config *Config
	canal  *canal.Canal
}

func NewTunnel(config *Config) *Tunnel {
	return &Tunnel{
		config: config,
	}
}

func (t *Tunnel) Start() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = t.config.MysqlAddr
	cfg.User = t.config.MysqlUser
	cfg.Password = t.config.MysqlPass
	cfg.ServerID = t.config.MysqlSlaveId

	canal, err := canal.NewCanal(cfg)
	if err != nil {
		return err
	}

	handler := new(Handler)
	handler.tunel = t
	err = handler.handlerInit()
	if err != nil {
		return err
	}

	canal.SetEventHandler(handler)

	pos, err := canal.GetMasterPos()
	if err != nil {
		return err
	}

	err = canal.StartFrom(pos)
	if err != nil {
		return err
	}

	t.canal = canal
	return nil
}

func (t *Tunnel) Stop() error {
	t.canal.Close()
	return nil
}
