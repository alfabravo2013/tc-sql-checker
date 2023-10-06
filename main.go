package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	qe "testcontainers-demo/queryexecutor"

	"github.com/gin-gonic/gin"
)

func main() {
	poolSize := GetMyConfig().PoolSize
	qe.InitContainerPool(poolSize)

	r := gin.Default()

	r.GET("/query", func(c *gin.Context) {

		results, err := qe.ExecuteQuery("SELECT * FROM testdb")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			if len(results) > 1 {
				c.JSON(200, gin.H{"columns": results[0], "rows": results[1:]})
			} else if len(results) > 0 {
				c.JSON(200, gin.H{"columns": results[0], "rows": make([]string, 0)})
			} else {
				emptySlice := make([]string, 0)
				c.JSON(200, gin.H{"columns": emptySlice, "rows": emptySlice})
			}
		}
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting server down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	qe.ShutDown()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v\n", err)
	}

	log.Println("Server exiting")
}
