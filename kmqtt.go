package kmqtt

import (
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type KClient struct {
	addr       string
	topics     []string
	opt        *mqtt.ClientOptions
	client     mqtt.Client
	msg        mqtt.MessageHandler
	msgHandler map[string]mqtt.MessageHandler
	other      mqtt.MessageHandler
	except     mqtt.MessageHandler
}

func NewClient(addr, client string, options ...string) *KClient {
	opt := mqtt.NewClientOptions()
	opt.AddBroker(fmt.Sprintf("tcp://%s:%d", addr, 1883))
	opt.SetClientID(client)

	if len(options) == 2 {
		opt.SetUsername(options[0])
		opt.SetPassword(options[1])
	}

	ret := &KClient{
		addr:       addr,
		opt:        opt,
		topics:     make([]string, 0),
		msgHandler: make(map[string]mqtt.MessageHandler),
	}
	ret.onMessage()

	return ret
}

func (s *KClient) AddTopics(topics ...string) {
	for _, v := range topics {
		if _, ok := s.exists(v); ok {
			continue
		}
		s.topics = append(s.topics, v)
	}
}

func (s *KClient) DeleteTopics(topics ...string) {
	for _, v := range topics {
		if index, ok := s.exists(v); ok {
			s.topics = append(s.topics[:index], s.topics[index+1:]...)
		}
	}
}

func (s *KClient) ClearTopics() {
	s.topics = make([]string, 0)
}

func (s *KClient) exists(topic string) (int, bool) {
	for k, v := range s.topics {
		if v == topic {
			return k, true
		}
	}

	return 0, false
}

func (s *KClient) OnMessage(topic string, msg mqtt.MessageHandler) {
	s.msgHandler[topic] = msg
}

func (s *KClient) onMessage() {

	s.opt.SetDefaultPublishHandler(func(c mqtt.Client, m mqtt.Message) {
		if fun, ok := s.msgHandler[m.Topic()]; ok {
			fun(c, m)
		} else if s.except != nil {
			s.except(c, m)
		}
		if s.other != nil {
			s.other(c, m)
		}
	})

}

func (s *KClient) ExceptMessage(msg mqtt.MessageHandler) {
	s.except = msg
}

func (s *KClient) AllMessage(msg mqtt.MessageHandler) {
	s.other = msg
}

func (s *KClient) OnConnect(onconnect mqtt.OnConnectHandler) {
	s.opt.SetOnConnectHandler(onconnect)
}

func (s *KClient) OnConnectionLost(connectlost mqtt.ConnectionLostHandler) {
	s.opt.SetConnectionLostHandler(connectlost)
}

func (s *KClient) Sub(qos byte) {
	for _, v := range s.topics {
		go s.sub(v, qos)
	}
}

func (s *KClient) AddSub(topic string, qos byte) {
	s.AddTopics(topic)
	go s.sub(topic, qos)
}

func (s *KClient) sub(v string, qos byte) {
	token := s.client.Subscribe(v, qos, s.msg)
	token.Wait()
}

func (s *KClient) Connect() {
	s.client = mqtt.NewClient(s.opt)
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (s *KClient) Close() {
	s.client.Disconnect(250)
}

func (s *KClient) Publish(text string, qos byte, topics ...string) {
	for _, v := range topics {
		token := s.client.Publish(v, qos, false, text)
		token.Wait()
	}
}

func (s *KClient) SetOptions(optfunc func(opt *mqtt.ClientOptions)) {
	optfunc(s.opt)
}
