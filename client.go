package kvclient

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"

	kv "github.com/strimertul/kilovolt/v3"
)

var (
	ErrNotAuthenticated     = errors.New("not authenticated")
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

type Client struct {
	Endpoint string
	Logger   logrus.FieldLogger

	headers       http.Header
	ws            *websocket.Conn
	mu            sync.Mutex // Used to avoid concurrent writes to socket
	requests      map[string]chan<- string
	subscriptions map[string][]chan<- string
}

type ClientOptions struct {
	Headers http.Header
	Logger  logrus.FieldLogger
}

func NewClient(endpoint string, options ClientOptions) (*Client, error) {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}

	client := &Client{
		Endpoint:      endpoint,
		Logger:        options.Logger,
		headers:       options.Headers,
		ws:            nil,
		mu:            sync.Mutex{},
		requests:      make(map[string]chan<- string),
		subscriptions: make(map[string][]chan<- string),
	}

	err := client.ConnectToWebsocket()

	return client, err
}

func (s *Client) Close() {
	if s.ws != nil {
		s.ws.Close()
	}
}

func (s *Client) ConnectToWebsocket() error {
	uri, err := url.Parse(s.Endpoint)
	if err != nil {
		return err
	}
	if uri.Scheme == "https" {
		uri.Scheme = "wss"
	} else {
		uri.Scheme = "ws"
	}

	s.ws, _, err = websocket.DefaultDialer.Dial(uri.String(), s.headers)
	if err != nil {
		return err
	}

	go func() {
		s.Logger.Debug("connected to ws, reading")
		for {
			mtype, message, err := s.ws.ReadMessage()
			if err != nil {
				s.Logger.WithError(err).Error("websocket read error")
				return
			}
			if mtype != websocket.TextMessage {
				continue
			}

			submessages := strings.Split(string(message), "\n")
			for _, msg := range submessages {
				var response kv.Response
				err = jsoniter.ConfigFastest.UnmarshalFromString(msg, &response)
				if err != nil {
					s.Logger.WithError(err).Error("websocket deserialize error")
					return
				}
				// Check message
				if response.RequestID != "" {
					// We have a request ID, send byte chunk over to channel
					if chn, ok := s.requests[response.RequestID]; ok {
						s.Logger.WithField("rid", response.RequestID).Trace("recv response")
						chn <- msg
					} else {
						s.Logger.WithField("rid", response.RequestID).Error("received response for unknown RID")
					}
				} else {
					// Might be a push
					switch response.CmdType {
					case "push":
						var push kv.Push
						err = jsoniter.ConfigFastest.UnmarshalFromString(msg, &push)
						s.Logger.WithField("key", push.Key).Trace("recv push")
						if err != nil {
							s.Logger.WithError(err).Error("websocket deserialize error")
							continue
						}
						// Deliver to subscriptions
						for sub, chans := range s.subscriptions {
							if push.Key != sub {
								continue
							}

							for _, chann := range chans {
								chann <- push.NewValue
							}
						}
					}
				}
			}
		}
	}()

	return nil
}

func (s *Client) GetKey(key string) (string, error) {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdReadKey,
		Data: map[string]interface{}{
			"key": key,
		},
	})
	if err != nil {
		return "", err
	}
	return resp.Data.(string), nil
}

func (s *Client) GetJSON(key string, dst interface{}) error {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdReadKey,
		Data: map[string]interface{}{
			"key": key,
		},
	})
	if err != nil {
		return err
	}

	return jsoniter.ConfigFastest.UnmarshalFromString(resp.Data.(string), dst)
}

func (s *Client) SetKey(key string, data string) error {
	_, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdWriteKey,
		Data: map[string]interface{}{
			"key":  key,
			"data": data,
		},
	})

	return err
}

func (s *Client) SetJSON(key string, data interface{}) error {
	serialized, err := jsoniter.ConfigFastest.MarshalToString(data)
	if err != nil {
		return err
	}

	_, err = s.makeRequest(kv.Request{
		CmdName: kv.CmdReadKey,
		Data: map[string]interface{}{
			"key":  key,
			"data": serialized,
		},
	})

	return err
}

func (s *Client) Subscribe(key string) (chan string, error) {
	chn := make(chan string)

	subs, ok := s.subscriptions[key]

	needsAPISubscription := !ok || len(subs) < 1
	s.subscriptions[key] = append(subs, chn)

	var err error
	// If this is the first time we subscribe to this key, ask server to push updates
	if needsAPISubscription {
		_, err = s.makeRequest(kv.Request{
			CmdName: kv.CmdSubscribeKey,
			Data: map[string]interface{}{
				"key": key,
			},
		})
	}

	return chn, err
}

func (s *Client) Unsubscribe(key string, chn chan string) error {
	if _, ok := s.subscriptions[key]; !ok {
		return nil
	}

	found := false
	for idx, sub := range s.subscriptions[key] {
		if sub == chn {
			s.subscriptions[key] = append(s.subscriptions[key][:idx], s.subscriptions[key][idx+1:]...)
			found = true
		}
	}

	if !found {
		return ErrSubscriptionNotFound
	}

	// If we removed all subscribers, ask server to not push updates to us anymore
	if len(s.subscriptions[key]) < 1 {
		_, err := s.makeRequest(kv.Request{
			CmdName: kv.CmdUnsubscribeKey,
			Data: map[string]interface{}{
				"key": key,
			},
		})
		return err
	}

	return nil
}

func (s *Client) makeRequest(request kv.Request) (kv.Response, error) {
	rid := ""
	for {
		rid = fmt.Sprintf("%x", rand.Int63())
		if _, ok := s.requests[rid]; ok {
			continue
		}
		break
	}

	responseChannel := make(chan string)
	s.requests[rid] = responseChannel

	request.RequestID = rid
	err := s.send(request)
	s.Logger.WithFields(logrus.Fields{
		"rid": request.RequestID,
		"cmd": request.CmdName,
	}).Trace("sent request")
	if err != nil {
		return kv.Response{}, err
	}

	// Wait for reply
	message := <-responseChannel

	var response kv.Response
	err = jsoniter.ConfigFastest.UnmarshalFromString(message, &response)

	if !response.Ok {
		var resperror kv.Error
		err = jsoniter.ConfigFastest.UnmarshalFromString(message, &resperror)
		if err != nil {
			return kv.Response{}, err
		}
		return kv.Response{}, fmt.Errorf("%s: %s", resperror.Error, resperror.Details)
	}

	return response, err
}

func (s *Client) send(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.ws.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	err1 := jsoniter.ConfigFastest.NewEncoder(w).Encode(v)
	err2 := w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
