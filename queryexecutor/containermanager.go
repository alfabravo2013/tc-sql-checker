package queryexecutor

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/lib/pq"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var ctx context.Context
var quit chan struct{}
var containerChan chan tc.Container

func InitContainerPool(poolSize uint8) {
	ctx = context.Background()

	quit = make(chan struct{})
	containerChan = make(chan tc.Container, poolSize)

	go func() {
		for {
			time.Sleep(20 * time.Millisecond)
			select {
			case <-quit:
				return
			default:
				if len(containerChan) < int(poolSize) {
					container, err := getDbContainer()
					if err != nil {
						log.Printf("Failed to create container: %v\n", err)
					} else {
						containerChan <- container
					}
				}
			}
		}
	}()
}

func ShutDown() {

	close(quit)
	// this time must be longer than the timeout in the tc producer goroutine
	// to avoid adding more containers while they are terminated in this function
	time.Sleep(100 * time.Microsecond)

	remaining := len(containerChan)
	for i := 0; i < remaining; i++ {
		cnt := <-containerChan
		if err := cnt.Terminate(ctx); err != nil {
			log.Printf("Failed to terminate the container: %v\n", err)
		}
	}

	close(containerChan)
}

func ExecuteQuery(query string) ([][]string, error) {
	container := <-containerChan
	connString, err := getConnectionString(container)
	if err != nil {
		return nil, fmt.Errorf("getting connection string: %w", err)
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("connecting database: %w", err)
	}

	stmt, err := db.Prepare(query)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			return nil, fmt.Errorf("preparing statement, %w, PG error code: %v", err, pqErr.Code)
		}
		return nil, fmt.Errorf("preparing statement: %w", err)
	}

	// Execute the SQL statement
	rows, err := stmt.Query()
	if err != nil {
		return nil, fmt.Errorf("executing statement: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting result set columns: %w", err)
	}

	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols))
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	var allResults [][]string
	allResults = append(allResults, cols)
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return nil, fmt.Errorf("reading raw rows: %w", err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}

		rowResult := make([]string, len(result))
		copy(rowResult, result)
		allResults = append(allResults, rowResult)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("checking result set errors: %w", err)
	}

	defer func() {
		db.Close()
		if err := container.Terminate(ctx); err != nil {
			log.Printf("Failed to terminate the container: %v\n", err)
		}
	}()

	return allResults, nil
}

func getDbContainer() (tc.Container, error) {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Cannot get current working dir: %v\n", err)
	}

	req := tc.ContainerRequest{
		Image:        "postgres:14.8-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "dev",
			"POSTGRES_PASSWORD": "dev",
		},
		// waiting for listened port does not always produces a ready to use database
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(20 * time.Second),
		Mounts: tc.Mounts(tc.ContainerMount{
			Source: tc.GenericBindMountSource{
				HostPath: path.Join(cwd, "init-script.sql"),
			},
			Target: "/docker-entrypoint-initdb.d/init-script.sql",
		}),
	}

	pgContainer, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, fmt.Errorf("running the container: %w", err)
	}
	return pgContainer, nil
}

func getConnectionString(container tc.Container) (string, error) {
	prt, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", fmt.Errorf("getting container's mapped port: %w", err)
	}

	hostIp, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("getting container's hostname: %w", err)
	}

	return fmt.Sprintf("postgresql://dev:dev@%s:%s?sslmode=disable", hostIp, prt.Port()), nil
}
