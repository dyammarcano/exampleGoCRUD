package main

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
)

const (
	SQL_CREATE_TABLE = `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY , name TEXT, age INTEGER, email TEXT, phone TEXT, createAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`
	SQL_INSERT_USER  = `INSERT INTO users (name, age, email, phone) VALUES (?, ?, ?, ?);`
	SQL_SELECT_USERS = `SELECT * FROM users;`
	SQL_DELETE_USER  = `DELETE FROM users WHERE id = ?;`
	SQL_UPDATE_USER  = `UPDATE users SET name = ?, age = ?, email = ?, phone = ? WHERE id = ?;`

	SQL_CREATE_ID = `CREATE TABLE IF NOT EXISTS hash_id (id INTEGER PRIMARY KEY AUTOINCREMENT, hash_id TEXT, createAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updateAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`
	SQL_INSERT_ID = `INSERT INTO hash_id (hash_id) VALUES (?);`
	SQL_UPDATE_ID = `UPDATE hash_id SET (hash_id, updateAt) VALUES (?, ?);`
	SQL_SELECT_ID = `SELECT * FROM hash_id WHERE hash_id = ?;`
)

type DataProvider struct {
	*sqlx.DB
}

func NewDataProvider(ddriverName, dataSourceName string) (*DataProvider, error) {
	db, err := sqlx.Open(ddriverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	if _, err = db.Exec(SQL_CREATE_TABLE); err != nil {
		return nil, err
	}

	if _, err = db.Exec(SQL_CREATE_ID); err != nil {
		return nil, err
	}

	return &DataProvider{
		DB: db,
	}, nil
}

func (p *DataProvider) AddUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	user := &User{
		ID: uuid.New().String(),
	}

	if err := json.NewDecoder(r.Body).Decode(user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := p.Exec(SQL_INSERT_ID, user.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err = p.Exec(SQL_INSERT_USER, id, user.Name, user.Age, user.Email, user.Phone); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err = json.NewEncoder(w).Encode(user); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (p *DataProvider) GetUsersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := p.Queryx(SQL_SELECT_USERS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var users = make([]User, 0)
	for rows.Next() {
		user := User{}
		if err = rows.StructScan(&user); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	w.Header().Set("Content-Type", "application/json")

	if err = json.NewEncoder(w).Encode(users); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (p *DataProvider) DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "ID is required", http.StatusBadRequest)
		return
	}

	result, err := p.Exec(SQL_SELECT_ID, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err = result.RowsAffected(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (p *DataProvider) UpdateUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var user User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := p.NamedExec(SQL_UPDATE_USER, user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err = result.LastInsertId(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err = json.NewEncoder(w).Encode(user); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type User struct {
	ID       string `json:"id"`
	RecordID int64  `json:"-" db:"id"`
	Name     string `json:"name" db:"name"`
	Email    string `json:"email" db:"email"`
	Phone    string `json:"phone" db:"phone"`
	Age      int    `json:"age" db:"age"`
}

func main() {
	provider, err := NewDataProvider("sqlite3", "file:users.sqlite3?cache=shared&_foreign_keys=1")
	if err != nil {
		log.Fatal(err)
	}
	defer provider.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/user/add", provider.AddUserHandler)
	mux.HandleFunc("/user/delete", provider.DeleteUserHandler)
	mux.HandleFunc("/user/list", provider.GetUsersHandler)
	mux.HandleFunc("/user/update", provider.UpdateUserHandler)

	log.Println("Server is running on port 8080")

	log.Fatal(http.ListenAndServe(":8080", mux))
}
