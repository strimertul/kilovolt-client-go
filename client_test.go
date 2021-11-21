package kvclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"

	kv "github.com/strimertul/kilovolt/v6"
)

func TestCommands(t *testing.T) {
	log := logrus.New()
	log.Level = logrus.TraceLevel

	server, _ := createInMemoryKV(t, log)

	client, err := NewClient(server.URL, ClientOptions{
		Logger: log,
	})
	if err != nil {
		t.Fatal("error creating kv client", err.Error())
	}

	t.Run("SetKey", func(t *testing.T) {
		if err := client.SetKey("test", "test1234"); err != nil {
			t.Fatal("error setting key to string value", err.Error())
		}
	})
	t.Run("GetKey", func(t *testing.T) {
		val, err := client.GetKey("test")
		if err != nil {
			t.Fatal("error getting string key", err.Error())
		}
		if val != "test1234" {
			t.Fatalf("returned value is different than expected, expected=%s got=%s", "test1234", val)
		}
	})
	type RandomStruct struct {
		Value int64
		Other string
	}
	t.Run("SetJSON", func(t *testing.T) {
		if err := client.SetJSON("testjson", RandomStruct{
			Value: 1234,
			Other: "wow!",
		}); err != nil {
			t.Fatal("error setting key to JSON value", err.Error())
		}
	})
	t.Run("GetJSON", func(t *testing.T) {
		var rnd RandomStruct
		if err := client.GetJSON("testjson", &rnd); err != nil {
			t.Fatal("error getting JSON key", err.Error())
		}
		if rnd.Value != 1234 || rnd.Other != "wow!" {
			t.Fatal("deserialized JSON has different values than expected")
		}
	})

	t.Run("SetKeys", func(t *testing.T) {
		if err := client.SetKeys(map[string]string{
			"multi1": "value1",
			"multi2": "1234",
		}); err != nil {
			t.Fatal("error setting multiple keys", err.Error())
		}
	})

	t.Run("SetJSONs", func(t *testing.T) {
		if err := client.SetJSONs(map[string]interface{}{
			"multijson1": RandomStruct{
				Value: 1234,
				Other: "wow!",
			},
			"multijson2": RandomStruct{
				Value: 9999,
				Other: "AAAAA",
			},
		}); err != nil {
			t.Fatal("error setting multiple keys to JSON values", err.Error())
		}
	})

	t.Run("GetKeys", func(t *testing.T) {
		val, err := client.GetKeys([]string{"test", "multi2"})
		if err != nil {
			t.Fatal("error getting key list", err.Error())
		}
		testKey, ok := val["test"]
		if !ok {
			t.Fatal("expected response to contain test key but it doesn't")
		}
		if testKey != "test1234" {
			t.Fatal("test key has different value than expected")
		}
		testKey, ok = val["multi2"]
		if !ok {
			t.Fatal("expected response to contain multi2 key but it doesn't")
		}
		if testKey != "1234" {
			t.Fatal("multi2 key has different value than expected")
		}
	})

	t.Run("GetByPrefix", func(t *testing.T) {
		val, err := client.GetByPrefix("multi")
		if err != nil {
			t.Fatal("error getting keys by prefix", err.Error())
		}
		if len(val) < 4 {
			t.Fatal("returned less keys than expected")
		}
		testKey, ok := val["multi1"]
		if !ok {
			t.Fatal("expected response to contain multi1 key but it doesn't")
		}
		if testKey != "value1" {
			t.Fatal("multi1 key has different value than expected")
		}
		testKey, ok = val["multi2"]
		if !ok {
			t.Fatal("expected response to contain multi2 key but it doesn't")
		}
		if testKey != "1234" {
			t.Fatal("multi2 key has different value than expected")
		}
	})

	var chn chan KeyValuePair
	t.Run("SubscribeKey", func(t *testing.T) {
		var err error
		chn, err = client.SubscribeKey("test")
		if err != nil {
			t.Fatal("error subscribing to key", err.Error())
		}
	})
	t.Run("UnsubscribeKey", func(t *testing.T) {
		err := client.UnsubscribeKey("test", chn)
		if err != nil {
			t.Fatal("error unsubscribing from key", err.Error())
		}
	})

	t.Run("SubscribePrefix", func(t *testing.T) {
		var err error
		chn, err = client.SubscribePrefix("test")
		if err != nil {
			t.Fatal("error subscribing to prefix", err.Error())
		}
	})
	t.Run("UnsubscribePrefix", func(t *testing.T) {
		err := client.UnsubscribePrefix("test", chn)
		if err != nil {
			t.Fatal("error unsubscribing from prefix", err.Error())
		}
	})
}

func TestKeySubscription(t *testing.T) {
	log := logrus.New()
	log.Level = logrus.TraceLevel

	server, _ := createInMemoryKV(t, log)

	client, err := NewClient(server.URL, ClientOptions{
		Logger: log,
	})
	if err != nil {
		t.Fatal("error creating kv client", err.Error())
	}

	chn, err := client.SubscribeKey("subtest")
	if err != nil {
		t.Fatal("error subscribing to key", err.Error())
	}

	if err = client.SetKey("subtest", "testvalue1234"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}
	// Check for pushes
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("push took too long to arrive")
	case push := <-chn:
		if push.Key != "subtest" || push.Value != "testvalue1234" {
			t.Fatal("wrong value received", push)
		}
	}

	if err = client.UnsubscribeKey("subtest", chn); err != nil {
		t.Fatal("error unsubscribing from key", err.Error())
	}
}

func TestPrefixSubscription(t *testing.T) {
	log := logrus.New()
	log.Level = logrus.TraceLevel

	server, _ := createInMemoryKV(t, log)

	client, err := NewClient(server.URL, ClientOptions{
		Logger: log,
	})
	if err != nil {
		t.Fatal("error creating kv client", err.Error())
	}

	chn, err := client.SubscribePrefix("sub")
	if err != nil {
		t.Fatal("error subscribing to prefix", err.Error())
	}

	if err = client.SetKey("subAAAA", "testvalue56709"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}
	// Check for pushes
	select {
	case <-time.After(20 * time.Second):
		t.Fatal("push took too long to arrive")
	case push := <-chn:
		if push.Key != "subAAAA" || push.Value != "testvalue56709" {
			t.Fatal("wrong value received", push)
		}
	}

	if err = client.UnsubscribePrefix("sub", chn); err != nil {
		t.Fatal("error unsubscribing from prefix", err.Error())
	}
}

func TestKeyList(t *testing.T) {
	log := logrus.New()
	log.Level = logrus.TraceLevel

	server, _ := createInMemoryKV(t, log)

	client, err := NewClient(server.URL, ClientOptions{
		Logger: log,
	})
	if err != nil {
		t.Fatal("error creating kv client", err.Error())
	}

	if err = client.SetKey("test", "testvalue1234"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}
	if err = client.SetKey("multi1", "value1"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}
	if err = client.SetKey("multi2", "1234"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}

	list, err := client.ListKeys("multi")
	if err != nil {
		t.Fatal("error getting key list", err.Error())
	}
	if len(list) != 2 {
		t.Fatal("wrong number of keys returned", len(list))
	}
	if list[0] != "multi1" || list[1] != "multi2" {
		t.Fatal("wrong keys returned", list)
	}
}

func TestAuthentication(t *testing.T) {
	log := logrus.New()
	log.Level = logrus.TraceLevel

	// Create hub with password
	const password = "testPassword"
	server, hub := createInMemoryKV(t, log)
	hub.SetOptions(kv.HubOptions{
		Password: password,
	})

	// Create client with password option
	client, err := NewClient(server.URL, ClientOptions{
		Logger:   log,
		Password: password,
	})
	if err != nil {
		t.Fatal("error creating kv client", err.Error())
	}

	// Couple test operations to test if auth actually went correctly
	if err = client.SetKey("test", "testvalue1234"); err != nil {
		t.Fatal("error modifying key", err.Error())
	}
	if _, err = client.GetKey("test"); err != nil {
		t.Fatal("error getting key")
	}
}

func createInMemoryKV(t *testing.T, log logrus.FieldLogger) (*httptest.Server, *kv.Hub) {
	// Open in-memory DB
	options := badger.DefaultOptions("").WithInMemory(true).WithLogger(log)
	db, err := badger.Open(options)
	if err != nil {
		t.Fatal("db initialization failed", err.Error())
	}

	// Create hub with in-mem DB
	hub, err := kv.NewHub(db, kv.HubOptions{}, log)
	if err != nil {
		t.Fatal("hub initialization failed", err.Error())
	}
	go hub.Run()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kv.ServeWs(hub, w, r)
	}))

	return ts, hub
}
