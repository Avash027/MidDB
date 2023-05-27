package tests

import (
	"math/rand"
	"net"
	"testing"
	"time"
)

const NUM_OF_TEST = 1000000

var keys = make([]string, NUM_OF_TEST)
var values = make([]string, NUM_OF_TEST)

func BenchmarkTestPut(t *testing.B) {

	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < NUM_OF_TEST; i++ {
		keys[i] = getRandomString(10)
		values[i] = getRandomString(10)
	}

	for i := 0; i < NUM_OF_TEST; i++ {
		_, err := conn.Write([]byte("PUT " + keys[i] + " " + values[i] + "\n"))

		if err != nil {
			t.Error(err)
		}
	}

}

func BenchmarkTestGet(t *testing.B) {

	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < NUM_OF_TEST; i++ {
		_, err := conn.Write([]byte("GET " + keys[i] + "\n"))

		if err != nil {
			t.Error(err)
		}

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)

		if err != nil {
			t.Error(err)
		}

	}

}

func BenchmarkTestGetUDP(t *testing.B) {

	conn, err := net.Dial("udp", "localhost:1053")

	errorRate := 0

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < NUM_OF_TEST; i++ {
		_, err := conn.Write([]byte("GET " + keys[i] + "\n"))

		if err != nil {
			t.Error(err)
		}

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if string(buf) == "NOT FOUND" {
			errorRate++
		}

	}

}

func getRandomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	rand.Seed(time.Now().UnixNano())

	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}
