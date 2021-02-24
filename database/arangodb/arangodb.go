package arangodb

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"unicode/utf8"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/golang-migrate/migrate/v4/database"
)

var (
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoHttpSchema   = fmt.Errorf("no http schema")
	ErrNilConfig      = fmt.Errorf("no config")
)

func init() {
	db := ArangoDB{}

	database.Register("arangodb", &db)
}

type ArangoDB struct {
	client driver.Client
	db     driver.Database
	config *Config
}

type Config struct {
	DatabaseName string
}

func WithInstance(instance driver.Client, config *Config) (database.Driver, error) {
	ctx := context.Background()

	e, err := instance.DatabaseExists(ctx, config.DatabaseName)
	if err != nil {
		return nil, err
	}

	if e != true {
		instance.CreateDatabase(ctx, config.DatabaseName, nil)
	}

	db, err := instance.Database(ctx, config.DatabaseName)
	if err != nil {
		return nil, err
	}

	mc := &ArangoDB{
		client: instance,
		db:     db,
		config: config,
	}

	return mc, nil
}

func (a *ArangoDB) Open(dsn string) (database.Driver, error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	_, rune_size := utf8.DecodeRuneInString(uri.Path)
	dbname := uri.Path[rune_size:]

	if len(dbname) == 0 {
		return nil, ErrNoDatabaseName
	}

	m, _ := url.ParseQuery(uri.RawQuery)

	httpSchema := m.Get("httpSchema")
	if len(httpSchema) == 0 {
		return nil, ErrNoHttpSchema
	}

	host := uri.Host

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{fmt.Sprintf("%s://%s", httpSchema, host)},
		ConnLimit: 32,
	})

	if err != nil {
		return nil, err
	}

	username := uri.User.Username()
	password, _ := uri.User.Password()

	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(username, password),
	})

	if err != nil {
		return nil, err
	}

	mc, err := WithInstance(client, &Config{
		DatabaseName: dbname,
	})

	if err != nil {
		return nil, err
	}

	return mc, nil
}

func (a *ArangoDB) Close() error {
	ctx := context.Background()
	return a.client.Shutdown(ctx, true)
}

func (a *ArangoDB) Lock() error {
	return nil
}

func (a *ArangoDB) Unlock() error {
	return nil
}

func (a *ArangoDB) Run(migration io.Reader) error {
	return nil
}

func (a *ArangoDB) SetVersion(version int, dirty bool) error {
	return nil
}

func (a *ArangoDB) Version() (version int, dirty bool, err error) {
	return 0, false, nil
}

func (a *ArangoDB) Drop() error {
	return nil
}
