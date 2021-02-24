package arangodb

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4/dktesting"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady, Env: map[string]string{
		"ARANGO_ROOT_PASSWORD": "root",
	}}
	specs = []dktesting.ContainerSpec{
		{ImageName: "arangodb/arangodb:3.7.8", Options: opts},
	}
)

func arangodbConnectionString(host, port, httpSchema string) string {
	return fmt.Sprintf("arangodb://root:root@%s:%s/testMigration?httpSchema=%s", host, port, httpSchema)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{fmt.Sprintf("http://%s:%s", ip, port)},
	})
	if err != nil {
		log.Println(err)
		return false
	}

	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", "root"),
	})
	if err != nil {
		log.Println(err)
		return false
	}

	_, err = client.DatabaseExists(ctx, "_system") //as PING
	if err != nil {
		switch err {
		case io.EOF:
			return false
		default:
			log.Println(err)
		}
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := arangodbConnectionString(ip, port, "http")
		p := &ArangoDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

	})
}
