package main

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/garyburd/redigo/redis"
	"github.com/pote/redisurl"
	"golang.org/x/net/websocket"
	"log"
	"net/http"
	"os"
	"runtime"
)

var RedisPool *redis.Pool = SetupRedis()

func SetupRedis() *redis.Pool {
	pool, err := redisurl.NewPool(3, 400, "240s")
	if err != nil {
		panic(err)
	}

	return pool
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Printf("[Main] Initializing Philotic Network on %v core(s)\n", runtime.NumCPU())

	http.Handle("/", websocket.Handler(ServeWebSocket))
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}

func ServeWebSocket(ws *websocket.Conn) {
	_, channels, err := RoutingInfo(ws.Request().FormValue("token"))

	if err != nil {
		log.Fatal(err)
		ws.Close()
	}

	connectionId := uuid.New()
	for _, channel := range channels {
		go ReceiveMessages(channel, connectionId, ws)
		go DispatchMessages(channel, connectionId, ws)
	}
	select {}
}

func DispatchMessages(channel, identifier string, ws *websocket.Conn) {
	pubSub := redis.PubSubConn{Conn: RedisPool.Get()}
	pubSub.PSubscribe(channel + ":*")

	for {
		switch m := pubSub.Receive().(type) {
		case redis.PMessage:
			if m.Channel != channel+":"+identifier {
				websocket.Message.Send(ws, string(m.Data))
			}
		}
	}
}

func ReceiveMessages(channel, identifier string, ws *websocket.Conn) {
	for {
		var message string
		websocket.Message.Receive(ws, &message)

		c := RedisPool.Get()
		c.Do("PUBLISH", channel+":"+identifier, message)
		c.Close()
	}
}

func RoutingInfo(at string) (hub string, channels []string, err error) {
	token, err := ParseAccessToken(at)

	if err != nil {
		return
	}

	hub = token.Hub
	channels = token.Channels

	return
}
