package main

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type hive struct {
	Philotes   map[string]*Philote
	Connect    chan *Philote
	Disconnect chan *Philote
}

type hiveInfo struct {
	Version         string `json:"version"`
	GoArch          string `json:"go_arch"`
	GoOS            string `json:"go_os"`
	GoVersion       string `json:"go_version"`
	NumCPU          int    `json:"num_cpu"`
	UptimeSeconds   int64  `json:"uptime_in_seconds"`
	UptimeDays      int64  `json:"uptime_in_days"`
	TCPPort         string `json:"tcp_port"`
	PID             int    `json:"pid"`
	Connections     int    `json:"connections"`
	MaxConnections  int    `json:"max_connections"`
	ReadBufferSize  int    `json:"read_buffer_size"`
	WriteBufferSize int    `json:"write_buffer_size"`
	CheckOrigin     bool   `json:"check_origin"`
}

func NewHive() *hive {
	h := &hive{
		Philotes:   map[string]*Philote{},
		Connect:    make(chan *Philote),
		Disconnect: make(chan *Philote),
	}

	go h.MaintainPhiloteIndex()

	return h
}

func (h *hive) MaintainPhiloteIndex() {

	log.Debug("Starting bookeeper")

	for {
		select {

		// 新连接到达
		case p := <-h.Connect:

			// 如果超过最大连接数，close 掉
			if len(h.Philotes) >= Config.maxConnections {
				log.WithFields(log.Fields{"philote": p.ID}).Warn("MAX_CONNECTIONS limit reached, dropping new connection")
				p.disconnect()
			}
			log.WithFields(log.Fields{"philote": p.ID}).Debug("Registering Philote")

			// 反向注册
			p.Hive = h

			// 注册新连接到 h.Philotes<> 中
			h.Philotes[p.ID] = p

			// 启动新连接上的消息读、写处理
			go p.Listen()

		// 连接关闭
		case p := <-h.Disconnect:
			log.WithFields(log.Fields{"philote": p.ID}).Debug("Disconnecting Philote")
			// 取消注册
			delete(h.Philotes, p.ID)
			// 关闭连接
			p.disconnect()
		}
	}
}

// 请求入口函数
func (h *hive) ServeNewConnection(w http.ResponseWriter, r *http.Request) {

    // 获取 http 鉴权头部
	auth := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer"))
	if auth == "" {
		r.ParseForm()
		auth = r.Form.Get("auth")
		log.WithFields(log.Fields{"auth": auth}).Debug("Empty Authorization header, trying querystring #auth param")
	}

	accessKey, err := NewAccessKey(auth)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if accessKey.API && strings.HasPrefix(r.URL.Path, "/api") {
		h.ServeAPICall(w, r)
		return
	}

	// http => websocket
	connection, err := Config.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Warn("Can't upgrade connection")
		w.Write([]byte(err.Error()))
		return
	}

	// 连接封装
	philote := NewPhilote(accessKey, connection)

	// 连接注册
	h.Connect <- philote
}

func (h *hive) ServeAPICall(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && r.URL.Path == "/api/info" {
		info := h.Inspect()
		data, err := json.Marshal(info)
		if err != nil {
			w.WriteHeader(500)
			return
		}

		w.Write(data)
		return
	}

	w.WriteHeader(420)
	return
}

func (h *hive) Inspect() *hiveInfo {
	return &hiveInfo{
		Version:         VERSION,
		GoArch:          runtime.GOARCH,
		GoOS:            runtime.GOOS,
		GoVersion:       runtime.Version(),
		NumCPU:          runtime.NumCPU(),
		UptimeSeconds:   time.Now().Unix() - Config.launchUnixTime,
		UptimeDays:      (time.Now().Unix() - Config.launchUnixTime) / 60 / 60 / 24,
		TCPPort:         Config.port,
		PID:             os.Getpid(),
		Connections:     len(h.Philotes),
		MaxConnections:  Config.maxConnections,
		ReadBufferSize:  Config.readBufferSize,
		WriteBufferSize: Config.writeBufferSize,
		CheckOrigin:     Config.checkOrigin,
	}
}
