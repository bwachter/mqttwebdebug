package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/trace"
)

var (
	listenAddress = flag.String("listen",
		":9383",
		"listen address for HTTP API")

	mqttBroker = flag.String("mqtt_broker",
		"tcp://localhost:1883",
		"MQTT broker address for github.com/eclipse/paho.mqtt.golang")

	mqttTopic = flag.String("mqtt_topic",
		"#",
		"MQTT topic to match on")

	//go:embed html/index.tmpl.html
	indexTemplate string

	//go:embed html/topic.tmpl.html
	topicTemplate string

	//go:embed "html/messageList.tmpl.html"
	messageListTemplate string
)

// A message on a topic...
// Data is directly copied from mqtt.Message, for the most part.
type TopicMessage struct {
	// the topic it belongs to
	Topic      *TopicRepository
	ReceivedAt time.Time

	// message data below...

	Duplicate bool
	Qos       byte
	Retained  bool
	MessageID uint16
	Payload   []byte
}

type TopicRepository struct {
	// the topic name...
	Topic string

	// TODO: consider storing a topic 'state', updated on last message e.g.
	// Ephemeral (no Retained messages known)
	// Retained (has a current retailed message)
	// RetainedRemoved (retained message was revoked, this seems to conventionally be done via a retained zero byte payload)
	// RetainedExpired - nah, doesn't make sense. have a separate "janitor" service that wipes topics that expire past their TTL.

	// appended to, so ordered by arrival
	LastMessages []*TopicMessage
}

type debugController struct {
	lock sync.Mutex

	mqttClient mqtt.Client

	// last 'n' messages, across all topics
	lastMessages []*TopicMessage

	// map of topic data
	topics map[string]*TopicRepository
}

func (this *debugController) eventMessageHandler(_ mqtt.Client, m mqtt.Message) {
	this.lock.Lock()
	defer this.lock.Unlock()

	topicStr := m.Topic()
	//log.Printf("mqtt event: %s: %v", topicStr, string(m.Payload()))

	var topic *TopicRepository
	var ok bool
	if topic, ok = this.topics[topicStr]; !ok {
		topic = &TopicRepository{
			Topic: topicStr,
		}
		this.topics[topicStr] = topic
	}

	msg := &TopicMessage{
		Topic:      topic,
		ReceivedAt: time.Now(),
		Duplicate:  m.Duplicate(),
		Qos:        m.Qos(),
		Retained:   m.Retained(),
		MessageID:  m.MessageID(),
		Payload:    m.Payload(),
	}

	// append to all last messages, and slice it to keep it short.
	// TODO: make configurable via a command topic, default to unlimited?
	const maxGlobalMessages = 20

	const maxTopicMessages = 50

	this.lastMessages = append(this.lastMessages, msg)
	if len(this.lastMessages) >= maxGlobalMessages {
		this.lastMessages = this.lastMessages[len(this.lastMessages)-maxGlobalMessages:]
	}

	topic.LastMessages = append(topic.LastMessages, msg)
	if len(topic.LastMessages) >= maxTopicMessages {
		topic.LastMessages = topic.LastMessages[len(topic.LastMessages)-maxTopicMessages:]
	}
}

type FlatMessage struct {
	Topic     string
	TimeSince string
	Duplicate bool
	Qos       byte
	Retained  bool
	MessageID uint16
	Payload   []byte
}

func flattenLastMessages(msgs []*TopicMessage) []FlatMessage {
	allMsgs := []FlatMessage{}
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]

		timeSince := time.Now().Sub(msg.ReceivedAt)
		timeSinceStr := ""

		h := timeSince / time.Hour
		timeSince -= h * time.Hour
		if h > 0 {
			timeSinceStr += fmt.Sprintf("%dh", h)
		}

		m := timeSince / time.Minute
		timeSince -= m * time.Minute
		if m > 0 {
			timeSinceStr += fmt.Sprintf("%dm", m)
		}

		s := timeSince / time.Second
		timeSince -= s * time.Second
		if s > 0 {
			timeSinceStr += fmt.Sprintf("%ds", s)
		}

		ms := timeSince / time.Millisecond
		timeSince -= ms * time.Millisecond
		if ms > 0 {
			timeSinceStr += fmt.Sprintf("%dms", ms)
		}

		allMsgs = append(allMsgs, FlatMessage{
			Topic:     msg.Topic.Topic,
			TimeSince: timeSinceStr,
			Duplicate: msg.Duplicate,
			Qos:       msg.Qos,
			Retained:  msg.Retained,
			MessageID: msg.MessageID,
			Payload:   msg.Payload,
		})
	}
	return allMsgs
}

func (this *debugController) handleIndex(w http.ResponseWriter, r *http.Request) {
	log.Printf("GET %s", r.URL.Path)
	this.lock.Lock()
	defer this.lock.Unlock()

	type Topic struct {
		Topic       string
		LastPayload []byte
	}

	type IndexData struct {
		AllMessages []FlatMessage
		AllTopics   []Topic
	}

	var data IndexData
	data.AllMessages = flattenLastMessages(this.lastMessages)

	for _, topic := range this.topics {
		data.AllTopics = append(data.AllTopics, Topic{
			Topic:       topic.Topic,
			LastPayload: topic.LastMessages[len(topic.LastMessages)-1].Payload,
		})

		sort.Slice(data.AllTopics, func(i, j int) bool {
			return data.AllTopics[i].Topic < data.AllTopics[j].Topic
		})
	}

	// FIXME: only do this once, instead?
	var t = template.Must(template.New("index").Parse(indexTemplate))
	template.Must(t.New("messageList").Parse(messageListTemplate))

	replyWithTemplate(w, t, data)
}

func replyWithTemplate[T any](w http.ResponseWriter, t *template.Template, data T) {
	buf := bytes.NewBuffer(nil)
	err := t.Execute(buf, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s", buf.Bytes())
}

func (this *debugController) handleTopic(w http.ResponseWriter, r *http.Request) {
	topicStr := r.URL.Query().Get("topic")
	log.Printf("GET topic %s", topicStr)
	this.lock.Lock()
	defer this.lock.Unlock()

	var topic *TopicRepository
	var ok bool
	if topic, ok = this.topics[topicStr]; !ok {
		// relatively harmless; pass a blank topic to the template, so an empty topic can be sent to
		topic = &TopicRepository{Topic: topicStr}
	}

	type TopicData struct {
		Topic        string
		LastMessages []FlatMessage
	}

	var data TopicData
	data.Topic = topic.Topic
	data.LastMessages = flattenLastMessages(topic.LastMessages)

	// FIXME: only do this once, instead?
	var t = template.Must(template.New("topic").Parse(topicTemplate))
	template.Must(t.New("messageList").Parse(messageListTemplate))

	replyWithTemplate(w, t, data)
}

func (this *debugController) handlePublish(w http.ResponseWriter, r *http.Request) {
	topicStr := r.FormValue("topic")
	payload := r.FormValue("payload")
	log.Printf("handlePublish %s", topicStr)

	// FIXME: configure qos, retained
	this.mqttClient.Publish(
		topicStr,
		0,     /* qos */
		false, /* retained */
		payload)

	http.Redirect(w, r, "/topic?topic="+topicStr, http.StatusSeeOther)
}

func subscribe(mqttClient mqtt.Client, topic string, hdl mqtt.MessageHandler) error {
	const qosAtMostOnce = 0
	log.Printf("Subscribing to %s", topic)
	token := mqttClient.Subscribe(topic, qosAtMostOnce, hdl)
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("subscription failed: %v", err)
	}
	return nil
}

func mqttwebdebug() error {
	ctrl := &debugController{topics: make(map[string]*TopicRepository)}

	opts := mqtt.NewClientOptions().AddBroker(*mqttBroker)
	clientID := "https://github.com/rburchell/mqttwebdebug"
	if hostname, err := os.Hostname(); err == nil {
		clientID += "@" + hostname
	}
	opts.SetClientID(clientID)
	opts.SetConnectRetry(true)
	opts.OnConnect = func(c mqtt.Client) {
		if err := subscribe(c, *mqttTopic, ctrl.eventMessageHandler); err != nil {
			log.Print(err)
		}
	}
	mqttClient := mqtt.NewClient(opts)
	ctrl.mqttClient = mqttClient // no need to lock; we aren't accepting requests yet.

	log.Printf("connecting to mqtt broker %s", *mqttBroker)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT connection failed: %v", token.Error())
	}
	log.Printf("connected to mqtt broker")

	trace.AuthRequest = func(req *http.Request) (any, sensitive bool) { return true, true }

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/requests/", trace.Traces)
	mux.HandleFunc("/topic", ctrl.handleTopic)
	mux.HandleFunc("/publish", ctrl.handlePublish)
	mux.HandleFunc("/", ctrl.handleIndex)

	log.Printf("http.ListenAndServe(%q)", *listenAddress)
	if err := http.ListenAndServe(*listenAddress, mux); err != nil {
		return err
	}
	return nil
}

func main() {
	flag.Parse()
	if err := mqttwebdebug(); err != nil {
		log.Fatal(err)
	}
}
