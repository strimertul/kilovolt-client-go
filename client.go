package kvclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	cmap "github.com/orcaman/concurrent-map"
	kv "github.com/strimertul/kilovolt/v11"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

var (
	ErrSubscriptionNotFound = errors.New("subscription not found")
	ErrEmptyKey             = errors.New("key empty or unset")
)

type KeyValuePair struct {
	Key   string
	Value string
}

type Client struct {
	Endpoint string
	Logger   *zap.Logger

	headers    http.Header
	ws         *websocket.Conn
	mu         sync.Mutex         // Used to avoid concurrent writes to socket
	requests   cmap.ConcurrentMap // map[string]chan<- string
	keysubs    cmap.ConcurrentMap // map[string][]chan<- KeyValuePair
	prefixsubs cmap.ConcurrentMap // map[string][]chan<- KeyValuePair
}

type ClientOptions struct {
	Headers  http.Header
	Password string
	Logger   *zap.Logger
}

func NewClient(endpoint string, options ClientOptions) (*Client, error) {
	if options.Logger == nil {
		options.Logger, _ = zap.NewProduction()
	}

	client := &Client{
		Endpoint:   endpoint,
		Logger:     options.Logger,
		headers:    options.Headers,
		ws:         nil,
		mu:         sync.Mutex{},
		requests:   cmap.New(), // make(map[string]chan<- string),
		keysubs:    cmap.New(), // make(map[string][]chan<- string),
		prefixsubs: cmap.New(), // make(map[string][]chan<- string),
	}

	err := client.ConnectToWebsocket()
	if err != nil {
		return nil, err
	}

	if options.Password != "" {
		err = client.Authenticate(options.Password)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (s *Client) Authenticate(password string) error {
	res, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdAuthRequest,
	})
	if err != nil {
		return err
	}

	data := res.Data.(map[string]interface{})

	// Decode challenge
	challengeBytes, err := base64.StdEncoding.DecodeString(data["challenge"].(string))
	if err != nil {
		return fmt.Errorf("failed to decode challenge: %w", err)
	}
	saltBytes, err := base64.StdEncoding.DecodeString(data["salt"].(string))
	if err != nil {
		return fmt.Errorf("failed to decode salt: %w", err)
	}

	// Create hash from password and challenge
	hash := hmac.New(sha256.New, append([]byte(password), saltBytes...))
	hash.Write(challengeBytes)
	hashBytes := hash.Sum(nil)

	// Send auth challenge
	_, err = s.makeRequest(kv.Request{
		CmdName: kv.CmdAuthChallenge,
		Data: map[string]interface{}{
			"hash": base64.StdEncoding.EncodeToString(hashBytes),
		},
	})
	return err
}

func (s *Client) Close() error {
	if s.ws != nil {
		return s.ws.CloseNow()
	}
	return nil
}

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

func (s *Client) readNext() (websocket.MessageType, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return s.ws.Read(ctx)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.ws, _, err = websocket.Dial(ctx, uri.String(), &websocket.DialOptions{
		HTTPHeader: s.headers,
	})
	if err != nil {
		return err
	}

	go func() {
		s.Logger.Debug("connected to ws, reading")
		for {
			mtype, message, err := s.readNext()
			if err != nil {
				s.Logger.Error("websocket read error", zap.Error(err))
				return
			}
			if mtype != websocket.MessageText {
				continue
			}

			submessages := strings.Split(string(message), "\n")
			for _, msg := range submessages {
				var response kv.Response
				err = jsoniter.ConfigFastest.UnmarshalFromString(msg, &response)
				if err != nil {
					s.Logger.Error("websocket deserialize error", zap.Error(err))
					return
				}
				// Check message
				if response.RequestID != "" {
					// We have a request ID, send byte chunk over to channel
					if chn, ok := s.requests.Get(response.RequestID); ok {
						s.Logger.Debug("recv response", zap.String("rid", response.RequestID))
						chn.(chan string) <- msg
						s.requests.Remove(response.RequestID)
					} else {
						s.Logger.Error("received response for unknown RID", zap.String("rid", response.RequestID))
					}
				} else {
					// Might be a push
					switch response.CmdType {
					case "push":
						var push kv.Push
						err = jsoniter.ConfigFastest.UnmarshalFromString(msg, &push)
						s.Logger.Debug("recv push", zap.String("key", push.Key))
						if err != nil {
							s.Logger.Error("websocket deserialize error", zap.Error(err))
							continue
						}
						// Deliver to key subscriptions
						if subs, ok := s.keysubs.Get(push.Key); ok {
							for _, chann := range subs.([]chan KeyValuePair) {
								chann <- KeyValuePair{push.Key, push.NewValue}
							}
						}
						// Deliver to prefix subscritpions
						for pair := range s.prefixsubs.IterBuffered() {
							if strings.HasPrefix(push.Key, pair.Key) {
								for _, chann := range pair.Val.([]chan KeyValuePair) {
									chann <- KeyValuePair{push.Key, push.NewValue}
								}
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

func (s *Client) GetKeys(keys []string) (map[string]string, error) {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdReadBulk,
		Data: map[string]interface{}{
			"keys": keys,
		},
	})
	if err != nil {
		return nil, err
	}

	vals := resp.Data.(map[string]interface{})
	toReturn := make(map[string]string)
	for k, v := range vals {
		toReturn[k] = v.(string)
	}
	return toReturn, nil
}

func (s *Client) GetByPrefix(prefix string) (map[string]string, error) {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdReadPrefix,
		Data: map[string]interface{}{
			"prefix": prefix,
		},
	})
	if err != nil {
		return nil, err
	}

	vals := resp.Data.(map[string]interface{})
	toReturn := make(map[string]string)
	for k, v := range vals {
		toReturn[k] = v.(string)
	}
	return toReturn, nil
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

	if resp.Data == nil || resp.Data.(string) == "" {
		return ErrEmptyKey
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

func (s *Client) SetKeys(data map[string]string) error {
	// This is so dumb
	toSet := make(map[string]interface{})
	for k, v := range data {
		toSet[k] = v
	}

	_, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdWriteBulk,
		Data:    toSet,
	})

	return err
}

func (s *Client) SetJSON(key string, data interface{}) error {
	serialized, err := jsoniter.ConfigFastest.MarshalToString(data)
	if err != nil {
		return err
	}

	_, err = s.makeRequest(kv.Request{
		CmdName: kv.CmdWriteKey,
		Data: map[string]interface{}{
			"key":  key,
			"data": serialized,
		},
	})

	return err
}

func (s *Client) SetJSONs(data map[string]interface{}) error {
	toSet := make(map[string]interface{})
	for k, v := range data {
		serialized, err := jsoniter.ConfigFastest.MarshalToString(v)
		if err != nil {
			return err
		}
		toSet[k] = serialized
	}

	_, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdWriteBulk,
		Data:    toSet,
	})

	return err
}

func (s *Client) SubscribeKey(key string) (chan KeyValuePair, error) {
	chn := make(chan KeyValuePair, 10)

	var subs []chan KeyValuePair
	data, ok := s.keysubs.Get(key)
	if ok {
		subs = data.([]chan KeyValuePair)
	}

	needsAPISubscription := !ok || len(subs) < 1
	s.keysubs.Set(key, append(subs, chn))

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

func (s *Client) UnsubscribeKey(key string, chn chan KeyValuePair) error {
	data, ok := s.keysubs.Get(key)
	if !ok {
		return nil
	}
	chans := data.([]chan KeyValuePair)

	found := false
	for idx, sub := range chans {
		if sub == chn {
			chans = append(chans[:idx], chans[idx+1:]...)
			s.keysubs.Set(key, chans)
			found = true
		}
	}

	if !found {
		return ErrSubscriptionNotFound
	}

	// If we removed all subscribers, ask server to not push updates to us anymore
	if len(chans) < 1 {
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

func (s *Client) SubscribePrefix(prefix string) (chan KeyValuePair, error) {
	chn := make(chan KeyValuePair, 10)

	var subs []chan KeyValuePair
	data, ok := s.prefixsubs.Get(prefix)
	if ok {
		subs = data.([]chan KeyValuePair)
	}

	needsAPISubscription := !ok || len(subs) < 1
	s.prefixsubs.Set(prefix, append(subs, chn))

	var err error
	// If this is the first time we subscribe to this key, ask server to push updates
	if needsAPISubscription {
		_, err = s.makeRequest(kv.Request{
			CmdName: kv.CmdSubscribePrefix,
			Data: map[string]interface{}{
				"prefix": prefix,
			},
		})
	}

	return chn, err
}

func (s *Client) UnsubscribePrefix(prefix string, chn chan KeyValuePair) error {
	data, ok := s.prefixsubs.Get(prefix)
	if !ok {
		return nil
	}
	chans := data.([]chan KeyValuePair)

	found := false
	for idx, sub := range chans {
		if sub == chn {
			chans = append(chans[:idx], chans[idx+1:]...)
			s.prefixsubs.Set(prefix, chans)
			found = true
		}
	}

	if !found {
		return ErrSubscriptionNotFound
	}

	// If we removed all subscribers, ask server to not push updates to us anymore
	if len(chans) < 1 {
		_, err := s.makeRequest(kv.Request{
			CmdName: kv.CmdUnsubscribePrefix,
			Data: map[string]interface{}{
				"prefix": prefix,
			},
		})
		return err
	}

	return nil
}

func (s *Client) ListKeys(prefix string) ([]string, error) {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdListKeys,
		Data: map[string]interface{}{
			"prefix": prefix,
		},
	})
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, k := range resp.Data.([]interface{}) {
		if key, ok := k.(string); ok {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (s *Client) InternalClientID() (int64, error) {
	resp, err := s.makeRequest(kv.Request{
		CmdName: kv.CmdInternalClientID,
	})
	if err != nil {
		return -1, err
	}
	return resp.Data.(int64), nil
}

func (s *Client) makeRequest(request kv.Request) (kv.Response, error) {
	rid := ""
	for {
		rid = fmt.Sprintf("%x", rand.Int63())
		if s.requests.Has(rid) {
			continue
		}
		break
	}

	responseChannel := make(chan string)
	s.requests.Set(rid, responseChannel)

	request.RequestID = rid
	err := s.send(request)
	s.Logger.Debug("sent request", zap.String("rid", request.RequestID), zap.String("cmd", request.CmdName))
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w, err := s.ws.Writer(ctx, websocket.MessageText)
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
