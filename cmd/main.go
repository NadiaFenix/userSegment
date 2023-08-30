package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type Segment struct {
	Slug string `json:"slug"`
}

type User struct {
	ID       string    `json:"id"`
	Segments []Segment `json:"segments"`
}

var db *sql.DB

func main() {
	// Установка соединения с базой данных
	connStr := "user=appuser password=password dbname=userdata port=8000 sslmode=disable"
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Создание таблицы пользователей
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(36) PRIMARY KEY,
        segments JSON
    )`)
	if err != nil {
		log.Fatal(err)
	}

	// Создание таблицы сегментов
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS segments (
        slug VARCHAR(50) PRIMARY KEY
    )`)
	if err != nil {
		log.Fatal(err)
	}

	// Инициализация роутера
	router := mux.NewRouter()

	// Установка обработчиков HTTP запросов
	router.HandleFunc("/segments", createSegment).Methods("POST")
	router.HandleFunc("/segments/{slug}", deleteSegment).Methods("DELETE")
	router.HandleFunc("/users/{id}/segments", addUserToSegment).Methods("PUT")
	router.HandleFunc("/users/segments/{id}", getActiveSegments).Methods("GET")

	// Запуск HTTP сервера
	log.Fatal(http.ListenAndServe(":8000", router))
}

func createSegment(w http.ResponseWriter, r *http.Request) {
	// Распарсить JSON из тела запроса
	var segment Segment
	err := json.NewDecoder(r.Body).Decode(&segment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Вставить сегмент в базу данных
	_, err = db.Exec("INSERT INTO segments (slug) VALUES (?)", segment.Slug)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправить успешный ответ
	w.WriteHeader(http.StatusCreated)
}

func deleteSegment(w http.ResponseWriter, r *http.Request) {
	// Получить slug сегмента из пути запроса
	vars := mux.Vars(r)
	slug := vars["slug"]

	// Удалить сегмент из базы данных
	_, err := db.Exec("DELETE FROM segments WHERE slug = ?", slug)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправить успешный ответ
	w.WriteHeader(http.StatusNoContent)
}

func addUserToSegment(w http.ResponseWriter, r *http.Request) {
	// Получить id пользователя из пути запроса
	vars := mux.Vars(r)
	userID := vars["id"]

	// Распарсить JSON из тела запроса
	var requestData struct {
		ToAdd    []string `json:"add"`
		ToRemove []string `json:"remove"`
	}
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Получить текущие сегменты пользователя из базы данных
	var segmentsJSON []byte
	err = db.QueryRow("SELECT segments FROM users WHERE id = ?", userID).Scan(&segmentsJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			segmentsJSON = []byte("[]")
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Распарсить JSON текущих сегментов пользователя
	var user User
	err = json.Unmarshal(segmentsJSON, &user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Добавить новые сегменты
	for _, slug := range requestData.ToAdd {
		user.Segments = append(user.Segments, Segment{Slug: slug})
	}

	// Удалить сегменты
	var newSegments []Segment
	for _, userSegment := range user.Segments {
		shouldRemove := false
		for _, slug := range requestData.ToRemove {
			if userSegment.Slug == slug {
				shouldRemove = true
				break
			}
		}
		if !shouldRemove {
			newSegments = append(newSegments, userSegment)
		}
	}

	// Преобразовать сегменты пользователя в JSON
	newSegmentsJSON, err := json.Marshal(newSegments)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Обновить список сегментов пользователя в базе данных
	_, err = db.Exec("INSERT INTO users (id, segments) VALUES (?, ?) ON DUPLICATE KEY UPDATE segments = ?", userID, newSegmentsJSON, newSegmentsJSON)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправить успешный ответ
	w.WriteHeader(http.StatusNoContent)
}

func getActiveSegments(w http.ResponseWriter, r *http.Request) {
	// Получить id пользователя из пути запроса
	vars := mux.Vars(r)
	userID := vars["id"]

	// Получить текущие сегменты пользователя из базы данных
	var segmentsJSON []byte
	err := db.QueryRow("SELECT segments FROM users WHERE id = ?", userID).Scan(&segmentsJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			segmentsJSON = []byte("[]")
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Распарсить JSON текущих сегментов пользователя
	var user User
	err = json.Unmarshal(segmentsJSON, &user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Преобразовать сегменты пользователя в JSON
	responseJSON, err := json.Marshal(user.Segments)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправить JSON в ответе
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, string(responseJSON))
}
