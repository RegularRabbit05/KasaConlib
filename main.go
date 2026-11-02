package main

import (
	"C"
	"fmt"
	"github.com/praserx/ipconv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func init() {
	HTTPClientHeaders = make(map[string]string)
}

func main() {
	ListenBroadcastDiscoveryMessages()
	for {
		ip := BroadcastDiscoveryRead()
		if ip != 0 {
			fmt.Println(ip)
			break
		}
	}
	CloseBroadcastDiscoveryMessages()
}

var BroadcastListenerServer net.PacketConn
var BroadcastListenerServerStarted = false
var BroadcastListenerServerStatus = false
var BroadcastListenerServerResult net.IP = nil

//export ListenBroadcastDiscoveryMessages
func ListenBroadcastDiscoveryMessages() {
	if BroadcastListenerServerStarted {
		return
	}
	BroadcastListenerServerResult = nil
	var err error
	BroadcastListenerServerStarted = true
	BroadcastListenerServerStatus = true
	BroadcastListenerServer, err = net.ListenPacket("udp", ":2025")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64)
		for {
			var cErr error
			var n int
			if !BroadcastListenerServerStarted {
				BroadcastListenerServerStatus = false
				break
			}
			_ = BroadcastListenerServer.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, _, cErr = BroadcastListenerServer.ReadFrom(buf)
			if cErr == nil {
				if strings.HasPrefix(string(buf[:n]), "mqttServerHello@") {
					split := strings.Split(string(buf[:n-1]), "@")
					if len(split) == 2 {
						var ip = net.ParseIP(split[1])
						if ip != nil {
							if !BroadcastListenerServerStarted {
								BroadcastListenerServerStatus = false
								return
							}
							BroadcastListenerServerStarted = false
							BroadcastListenerServerResult = ip
							_ = BroadcastListenerServer.Close()
							BroadcastListenerServerStatus = false
							return
						}
					}
				}
			}
		}
	}()
}

//export BroadcastDiscoveryRead
func BroadcastDiscoveryRead() uint32 {
	if BroadcastListenerServerResult == nil {
		return 0
	}
	ip, err := ipconv.IPv4ToInt(BroadcastListenerServerResult)
	if err != nil {
		return 0
	}
	return ip
}

//export CloseBroadcastDiscoveryMessages
func CloseBroadcastDiscoveryMessages() {
	if !BroadcastListenerServerStarted {
		return
	}
	BroadcastListenerServerStarted = false
	_ = BroadcastListenerServer.Close()
	for BroadcastListenerServerStatus {
	}
	BroadcastListenerServerStatus = false
	BroadcastListenerServerResult = nil
}

var HTTPClientHeaders map[string]string
var HTTPClientHeadersMutex sync.RWMutex

//export SetHTTPHeader
func SetHTTPHeader(key string, value string) {
	HTTPClientHeadersMutex.Lock()
	defer HTTPClientHeadersMutex.Unlock()
	if HTTPClientHeaders == nil {
		HTTPClientHeaders = make(map[string]string)
	}
	if value == "" {
		delete(HTTPClientHeaders, key)
		return
	}
	HTTPClientHeaders[key] = value
}

//export ClearHTTPHeaders
func ClearHTTPHeaders() {
	HTTPClientHeadersMutex.Lock()
	defer HTTPClientHeadersMutex.Unlock()
	HTTPClientHeaders = make(map[string]string)
}

//export RunHTTPRequest
func RunHTTPRequest(url string, method string, body string, responseBuffer *byte, responseBufferLen uint32) uint64 {
	HTTPClientHeadersMutex.RLock()
	defer HTTPClientHeadersMutex.RUnlock()
	httpRequest, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		log.Println("HTTP Request Error 1: ", err)
		return (0xffffffff << 32) | 2
	}
	for k, v := range HTTPClientHeaders {
		httpRequest.Header.Set(k, v)
	}
	httpResponse, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		log.Println("HTTP Request Error 2: ", err)
		return (0xffffffff << 32) | 3
	}
	httpResponseBody, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		log.Println("HTTP Request Error 3: ", err)
		return (0xffffffff << 32) | 4
	}
	if int(responseBufferLen) < len(httpResponseBody) {
		log.Println("HTTP Request Error 4: response buffer too short")
		return (0xffffffff << 32) | 5
	}
	var data []byte
	strRes := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	strRes.Data = uintptr(unsafe.Pointer(responseBuffer))
	strRes.Len = int(responseBufferLen)
	strRes.Cap = int(responseBufferLen)
	for i := 0; i < len(httpResponseBody) && i < int(responseBufferLen); i++ {
		data[i] = httpResponseBody[i]
	}
	return uint64(uint32(httpResponse.StatusCode))<<32 | uint64(uint32(httpResponse.ContentLength))
}

//export RunHTTPRequestAsync
func RunHTTPRequestAsync(url string, method string, body string, responseBuffer *byte, responseBufferLen uint32, code *uint64) {
	*code = 0
	var copiedData = make([]string, 3)
	var sb strings.Builder
	sb.WriteString(url)
	copiedData[0] = sb.String()
	sb.Reset()
	sb.WriteString(method)
	copiedData[1] = sb.String()
	sb.Reset()
	sb.WriteString(body)
	copiedData[2] = sb.String()
	go func() {
		*code = RunHTTPRequest(copiedData[0], copiedData[1], copiedData[2], responseBuffer, responseBufferLen)
	}()
}
