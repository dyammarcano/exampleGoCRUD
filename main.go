package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"io/fs"
	"log"
	"net/http"
	"time"
)

const (
	SqlCreateTable = `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, age INTEGER, email TEXT, phone TEXT, createAt TIMESTAMP);`
	SqlCreateId    = `CREATE TABLE IF NOT EXISTS uuid_map (id INTEGER PRIMARY KEY AUTOINCREMENT, uuid TEXT, user_id TEXT, createAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updateAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`
	SqlInsertUser  = `INSERT INTO users (username, age, email, phone, createAt) VALUES (?, ?, ?, ?, ?);`
	SqlInsertId    = `INSERT INTO uuid_map (user_id, uuid) VALUES (?, ?);`
	SqlSelectUsers = `SELECT * FROM users;`
	SqlSeletUer    = `SELECT u.id, u.username, u.age, u.email, u.phone, u.createAt FROM users u JOIN uuid_map m ON u.id = m.user_id WHERE m.uuid = ?;`
	SqlDeleteUser  = `DELETE FROM users WHERE id IN (SELECT user_id FROM uuid_map WHERE uuid = ?);`
	SqlDeleteId    = `DELETE FROM uuid_map WHERE uuid = ?;`
	SqlUpdateUser  = `UPDATE users SET username = ?, age = ?, email = ?, phone = ? WHERE id = ?;`
)

//go:embed app/dist/*
var content embed.FS

type DataProvider struct {
	*sqlx.DB
}

// NewDataProvider creates a new data provider with the given driver name and data source name.
func NewDataProvider(driverName, dataSourceName string) (*DataProvider, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	if _, err = db.Exec(SqlCreateTable); err != nil {
		return nil, err
	}
	if _, err = db.Exec(SqlCreateId); err != nil {
		return nil, err
	}
	return &DataProvider{
		DB: db,
	}, nil
}

type User struct {
	ID       int64  `json:"-"`
	UUID     string `json:"uuid"`
	Username string `json:"username" db:"username"`
	Email    string `json:"email" db:"email"`
	Phone    string `json:"phone" db:"phone"`
	Age      int    `json:"age" db:"age"`
	CreateAt string `json:"createAt" db:"createAt"`
}

func (p *DataProvider) AddUserHandler(w http.ResponseWriter, r *http.Request) {
	// check if the request method is POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// create a new user object with a unique uuid
	user := &User{}

	// decode the request body into user
	if err := json.NewDecoder(r.Body).Decode(user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// insert hash_id into hash_id table
	if err := createUser(p.DB, user); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// encode the user to json and write it to the response writer
	response(w, user)
}

func (p *DataProvider) GetUsersHandler(w http.ResponseWriter, r *http.Request) {
	// check if the request method is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// query the users from the users table
	rows, err := p.Queryx(SqlSelectUsers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// create a slice of users
	var users = make([]User, 0)

	// iterate over the rows
	for rows.Next() {
		user := User{}
		if err = rows.StructScan(&user); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	// encode the user to json and write it to the response writer
	response(w, users)
}

func (p *DataProvider) GetUserHandler(w http.ResponseWriter, r *http.Request) {
	// check if the request method is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// get the uuid from the query parameter
	uid := r.URL.Query().Get("id")
	if uid == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}

	// create a new user object
	user, err := getUserByUUID(p.DB, uid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, fmt.Sprintf("User with ID %s not found", uid), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// encode the user to json and write it to the response writer
	response(w, user)
}

func (p *DataProvider) DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	// check if the request method is DELETE
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// get the id from the query parameter
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}

	// delete the user from the users table
	if err := deleteUser(p.DB, id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (p *DataProvider) UpdateUserHandler(w http.ResponseWriter, r *http.Request) {
	// check if the request method is PUT
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// create a new user object
	var user User

	// decode the request body into user
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// update the user in the users table
	if err := updateUser(p.DB, &user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// encode the user to json and write it to the response writer
	response(w, user)
}

func main() {
	provider, err := NewDataProvider("sqlite3", "file:users.sqlite3?cache=shared&_foreign_keys=1")
	if err != nil {
		log.Fatal(err)
	}
	defer provider.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/user/add", provider.AddUserHandler)
	mux.HandleFunc("/user/get", provider.GetUserHandler)
	mux.HandleFunc("/user/delete", provider.DeleteUserHandler)
	mux.HandleFunc("/user/list", provider.GetUsersHandler)
	mux.HandleFunc("/user/update", provider.UpdateUserHandler)

	// frontend layer
	mux.Handle("/", http.StripPrefix("/", assetsHandler(content)))

	log.Println("Server is running on port 8080")

	log.Fatal(http.ListenAndServe(":8080", mux))
}

// response writes the user to the response writer.
func response(w http.ResponseWriter, v any) {
	// set header content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// encode the user to json and write it to the response writer
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func createUser(db *sqlx.DB, user *User) error {
	// Generate a new UUID and assign it to the user
	user.UUID = uuid.New().String()
	user.CreateAt = time.Now().Format(time.RFC3339)

	// Get the last inserted ID (user_id)
	result, err := db.Exec(SqlInsertUser, user.Username, user.Age, user.Email, user.Phone, user.CreateAt)
	if err != nil {
		return err
	}

	// Get the last inserted ID (user_id)
	userID, err := result.LastInsertId()
	if err != nil {
		return err
	}

	// Update uuid_map table with user_id
	_, err = db.Exec(SqlInsertId, userID, user.UUID)
	return err
}

func getUserByUUID(db *sqlx.DB, uuid string) (*User, error) {
	// get the user from the users table
	var user User

	// get the user from the users table
	if err := db.Get(&user, SqlSeletUer, uuid); err != nil {
		return nil, err
	}

	user.UUID = uuid

	return &user, nil
}

func updateUser(db *sqlx.DB, user *User) error {
	// get the user from the users table
	result, err := getUserByUUID(db, user.UUID)
	if err != nil {
		return err
	}
	// update the user in the users table
	if _, err = db.Exec(SqlUpdateUser, user.Username, user.Age, user.Email, user.Phone, result.ID); err != nil {
		return err
	}
	return err
}

func deleteUser(db *sqlx.DB, uid string) error {
	// delete the user from the users table
	if _, err := db.Exec(SqlDeleteUser, uid); err != nil {
		return err
	}
	// delete the id from the hash_id table
	if _, err := db.Exec(SqlDeleteId, uid); err != nil {
		return err
	}
	return nil
}

func assetsHandler(content embed.FS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub, err := fs.Sub(content, "app/dist")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fileServer := http.FileServer(http.FS(sub))
		fileServer.ServeHTTP(w, r)
	}
}
