package database

import (
	"fmt"
	env "lib/shared_lib/env"

	log "github.com/sirupsen/logrus"
)

type DatabaseConfig struct {
	host           string
	port           string
	name           string
	user           string
	password       string
	ssl            string
	maxConnections int
}

func (db DatabaseConfig) GetConnectionString() string {
	log.Trace("DatabaseConfig GetConnectionString")

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s&pool_max_conns=%d", db.user, db.password, db.host, db.port, db.name, db.ssl, db.maxConnections)
	return connString
}

func (db DatabaseConfig) GetListenerConnectionString() string {
	log.Trace("DatabaseConfig GetListenerConnectionString")

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", db.user, db.password, db.host, db.port, db.name, db.ssl)
}

func (db DatabaseConfig) GetName() string {
	log.Trace("DatabaseConfig GetName")
	return db.name
}

func (db *DatabaseConfig) WithDbName(dbName, defaultValue string) {
	db.name = env.WithDefaultString(dbName, defaultValue)
}

func (db *DatabaseConfig) RequireDbName(dbName string) {
	db.name = env.MustString(dbName)
}

func (db *DatabaseConfig) WithDbUser(user, defaultValue string) {
	db.user = env.WithDefaultString(user, defaultValue)
}

func (db *DatabaseConfig) RequireDbUser(user string) {
	db.user = env.MustString(user)
}

func (db *DatabaseConfig) WithDbPassword(password, defaultValue string) {
	db.password = env.WithDefaultString(password, defaultValue)
}

func (db *DatabaseConfig) RequireDbPassword(password string) {
	db.password = env.MustString(password)
}

func (db *DatabaseConfig) WithDbMaxConnections(maxConnections, defaultValue string) {
	db.maxConnections = env.WithDefaultInt(maxConnections, defaultValue)
}

func (db *DatabaseConfig) RequireDbMaxConnections(maxConnections string) {
	db.maxConnections = env.MustInt(maxConnections)
}

func (db *DatabaseConfig) WithDbHost(host, defaultValue string) {
	db.host = env.WithDefaultString(host, defaultValue)
}

func (db *DatabaseConfig) RequireDbHost(host string) {
	db.host = env.MustString(host)
}

func (db *DatabaseConfig) WithDbPort(port, defaultValue string) {
	db.port = env.WithDefaultString(port, defaultValue)
}

func (db *DatabaseConfig) RequireDbPort(port string) {
	db.port = env.MustString(port)
}

func (db *DatabaseConfig) WithDbSSLMode(sslMode, defaultValue string) {
	db.ssl = env.WithDefaultString(sslMode, defaultValue)
}

func (db *DatabaseConfig) RequireDbSSLMode(sslMode string) {
	db.ssl = env.MustString(sslMode)
}
