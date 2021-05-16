package kvclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"

	kv "github.com/strimertul/kilovolt/v4"
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
		if err := client.SetJSON("test", RandomStruct{
			Value: 1234,
			Other: "wow!",
		}); err != nil {
			t.Fatal("error setting key to JSON value", err.Error())
		}
	})
	t.Run("GetJSON", func(t *testing.T) {
		var rnd RandomStruct
		if err := client.GetJSON("test", &rnd); err != nil {
			t.Fatal("error getting JSON key", err.Error())
		}
		if rnd.Value != 1234 || rnd.Other != "wow!" {
			t.Fatal("deserialized JSON has different values than expected")
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

func createInMemoryKV(t *testing.T, log logrus.FieldLogger) (*httptest.Server, *kv.Hub) {
	// Open in-memory DB
	options := badger.DefaultOptions("").WithInMemory(true).WithLogger(log)
	db, err := badger.Open(options)
	if err != nil {
		t.Fatal("db initialization failed", err.Error())
	}

	// Create hub with in-mem DB
	hub, err := kv.NewHub(db, log)
	if err != nil {
		t.Fatal("hub initialization failed", err.Error())
	}
	go hub.Run()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kv.ServeWs(hub, w, r)
	}))

	return ts, hub
}
