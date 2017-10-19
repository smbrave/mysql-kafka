package tunnel

import (
	"encoding/json"
	"fmt"

	"strings"

	"log"

	"github.com/Shopify/sarama"
	"github.com/siddontang/go-mysql/canal"
)

type Handler struct {
	canal.DummyEventHandler
	tunel    *Tunnel
	producer sarama.SyncProducer
}

func (h *Handler) handlerInit() error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner

	producer, err := sarama.NewSyncProducer(strings.Split(h.tunel.config.KafkaAddr, ","), config)
	if err != nil {
		return err
	}
	h.producer = producer
	return nil
}

func (h *Handler) OnRow(e *canal.RowsEvent) error {

	switch e.Action {
	case canal.UpdateAction:
		return h.handlerUpdate(e)
	case canal.InsertAction:
		return h.handlerInsert(e)
	case canal.DeleteAction:
		return h.handlerDelete(e)
	}
	return nil
}

func (h *Handler) handerSend(data *TunelData) error {
	result, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := sarama.ProducerMessage{}
	msg.Topic = h.tunel.config.KafkaTopic
	msg.Partition = int32(-1)
	msg.Key = sarama.StringEncoder(data.Key)
	msg.Value = sarama.ByteEncoder(string(result))

	partition, offset, err := h.producer.SendMessage(&msg)
	if err != nil {
		return err
	}
	log.Printf("partition=%d, offset=%d\n", partition, offset)
	return nil
}

func (h *Handler) handlerParseData(e *canal.RowsEvent, idx int) ([]byte, error) {
	data := make(map[string]interface{})
	for i, col := range e.Table.Columns {
		data[col.Name] = e.Rows[idx][i]
	}
	return json.Marshal(data)
}

func (h *Handler) handlerUpdate(e *canal.RowsEvent) error {
	pk := e.Table.PKColumns
	for i := 0; i+1 < len(e.Rows); i += 2 {
		data := new(TunelData)
		data.Database = e.Table.Schema
		data.Table = e.Table.Name
		data.Action = e.Action

		//主键
		if len(pk) >= 0 {
			data.Key = fmt.Sprintf("%v", e.Rows[i][pk[0]])
		}

		//老数据
		body, err := h.handlerParseData(e, i)
		if err != nil {
			log.Println("handlerParseData err:", err)
			continue
		}
		data.OData = string(body)

		//更新数据
		body, err = h.handlerParseData(e, i+1)
		if err != nil {
			log.Println("handlerParseData err :", err)
			continue
		}
		data.Data = string(body)

		if err := h.handerSend(data); err != nil {
			log.Println("send data err", err)
		}
	}
	return nil
}

func (h *Handler) handlerInsert(e *canal.RowsEvent) error {
	pk := e.Table.PKColumns
	for i := 0; i < len(e.Rows); i++ {
		data := new(TunelData)
		data.Database = e.Table.Schema
		data.Table = e.Table.Name
		data.Action = e.Action

		//主键
		if len(pk) >= 0 {
			data.Key = fmt.Sprintf("%v", e.Rows[i][pk[0]])
		}

		//更新数据
		body, err := h.handlerParseData(e, i)
		if err != nil {
			continue
		}
		data.Data = string(body)

		if err := h.handerSend(data); err != nil {
			log.Println("send data err", err)
		}

	}
	return nil
}

func (h *Handler) handlerDelete(e *canal.RowsEvent) error {
	return h.handlerInsert(e)
}
