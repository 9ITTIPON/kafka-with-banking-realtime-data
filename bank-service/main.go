package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

type Transaction struct {
	ID        int       `json:"id"`
	AccountID string    `json:"account_id"`
	Amount    float64   `json:"amount"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

var db *sql.DB

func initDB() {
	var err error
	dbHost := "localhost"
	if host := os.Getenv("DB_HOST"); host != "" {
		dbHost = host
	}
	connStr := fmt.Sprintf("host=%s port=5432 user=bankuser password=bankpass dbname=bank_db sslmode=disable", dbHost)
	
	// Wait for DB to be ready
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		log.Printf("Waiting for database... %d/10", i+1)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatal(err)
	}

	// Create table
	query := `
	CREATE TABLE IF NOT EXISTS transactions (
		id SERIAL PRIMARY KEY,
		account_id VARCHAR(50) NOT NULL,
		amount DECIMAL(15, 2) NOT NULL,
		type VARCHAR(20) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database initialized and table created.")
}

func createTransaction(c *gin.Context) {
	var tx Transaction
	if err := c.ShouldBindJSON(&tx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	query := `INSERT INTO transactions (account_id, amount, type) VALUES ($1, $2, $3) RETURNING id, created_at`
	err := db.QueryRow(query, tx.AccountID, tx.Amount, tx.Type).Scan(&tx.ID, &tx.CreatedAt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, tx)
}

func main() {
	initDB()

	r := gin.Default()
	r.POST("/transaction", createTransaction)

	fmt.Println("Bank service running on :8080")
	r.Run(":8080")
}
