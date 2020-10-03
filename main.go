package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"net/http"

	"github.com/oklog/ulid"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// Config configuration struct
type Config struct {
	Brokers []string
	Topic   goka.Stream
	Group   goka.Group
	Consume goka.Stream
	Port    string
	Host    string
}

// GetSrvAddr returns a string HOST:PORT
func (c *Config) GetSrvAddr() string {
	srvaddr := c.Host + ":" + c.Port
	return srvaddr
}

// NewConfig returns a new instance of Config
func NewConfig(b string, t string, g string, c string, h string, p string) (*Config, error) {
	return &Config{
		Brokers: strings.Split(b, ","),
		Topic:   goka.Stream(t),
		Group:   goka.Group(g),
		Consume: goka.Stream(c),
		Host:    h,
		Port:    p,
	}, nil
}

// GetEnv returns an env variable value or a default
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// NewULID returns a ULID as a string.
func NewULID() string {
	newid, _ := ulid.New(ulid.Timestamp(time.Now()), rand.Reader)
	return newid.String()
}

// A user is the object that is stored in the processor's group table
type user struct {
	// number of clicks the user has performed.
	Clicks int
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

// Encodes a user into []byte
func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation.
func (jc *userCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   user
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}
	return &c, nil
}

func runEmitter(cfg *Config) {
	emitter, err := goka.NewEmitter(cfg.Brokers, cfg.Topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var i int
	for range t.C {
		key := fmt.Sprintf("user-%d", i%10)
		value := fmt.Sprintf("%s", time.Now())
		emitter.EmitSync(key, value)
		i++
	}
}

func process(ctx goka.Context, msg interface{}) {
	var u *user
	if val := ctx.Value(); val != nil {
		u = val.(*user)
	} else {
		u = new(user)
	}

	u.Clicks++
	ctx.SetValue(u)
	fmt.Printf("[proc] key: %s clicks: %d, msg: %v\n", ctx.Key(), u.Clicks, msg)
}

func runProcessor(cfg *Config) {
	g := goka.DefineGroup(cfg.Group,
		goka.Input(cfg.Topic, new(codec.String), process),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(cfg.Brokers, g)
	if err != nil {
		panic(err)
	}

	p.Run(context.Background())
}

func runView(cfg *Config) {
	view, err := goka.NewView(cfg.Brokers,
		goka.GroupTable(cfg.Group),
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		w.Write(data)
	})

	fmt.Printf("View opened at %s", cfg.GetSrvAddr())
	go http.ListenAndServe(cfg.GetSrvAddr(), root)

	view.Run(context.Background())
}

var (
	brokers string
	consume string
	topic   string
	group   string
	port    string
	host    string
)

func init() {
	flag.StringVar(&brokers, "brokers", GetEnv("KAFKA_PEERS", "localhost:9092"), "The Kafka brokers to connect to, as a comma separated list")
	flag.StringVar(&consume, "consume", GetEnv("AUTOEUREKA_CONSUMER_TOPIC", "autoeureka"), "The topic to produce messages to")
	flag.StringVar(&topic, "topic", GetEnv("AUTOEUREKA_PRODUCER_TOPIC", "autoeureka"), "The topic to produce messages to")
	flag.StringVar(&group, "group", GetEnv("AUTOEUREKA_CONSUMER_GROUP", NewULID()), "consumer group id")
	flag.StringVar(&host, "host", GetEnv("AUTOEUREKA_HOST", "localhost"), "Autoeureka")
	flag.StringVar(&port, "port", GetEnv("AUTOEUREKA_PORT", "9999"), "Autoeureka Server Port")
	flag.Parse()
}

func main() {
	fmt.Printf("Brokers: %s\n", brokers)
	fmt.Printf("Consume: %s\n", consume)
	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Group: %s\n", group)
	fmt.Printf("Host: %s\n", host)
	fmt.Printf("Port: %s\n", port)
	cfg, _ := NewConfig(brokers, topic, group, consume, host, port)
	go runEmitter(cfg)
	go runProcessor(cfg)
	runView(cfg)
}
