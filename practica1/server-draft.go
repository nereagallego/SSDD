/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 1
 */
package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"

	"practica1/com"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func IsPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func FindPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if IsPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

// sendRequest envía una petición (id, interval) al servidor. Una petición es un par id
// (el identificador único de la petición) e interval, el intervalo en el cual se desea que el servidor encuentre los
// números primos. La petición se serializa utilizando el encoder y una vez enviada la petición
// se almacena en una estructura de datos, junto con una estampilla
// temporal. Para evitar condiciones de carrera, la estructura de datos compartida se almacena en una Goroutine
// (handleRequests) y que controla los accesos a través de canales síncronos. En este caso, se añade una nueva
// petición a la estructura de datos mediante el canal addChan
func sendReply(id int, primes []int, encoder *gob.Encoder) {
	reply := com.Reply{id, primes}

	err := encoder.Encode(&reply)
	checkError(err)
}

// receiveReply recibe las respuestas (id, primos) del servidor. Respuestas que corresponden con peticiones previamente
// realizadas.
// el encoder y una vez enviada la petición se almacena en una estructura de datos, junto con una estampilla
// temporal. Para evitar condiciones de carrera, la estructura de datos compartida se almacena en una Goroutine
// (handleRequests) y que controla los accesos a través de canales síncronos. En este caso, se añade una nueva
// petición a la estructura de datos mediante el canal addChan
func receiveRequest(decoder *gob.Decoder, encoder *gob.Encoder) {
	for {
		var request com.Request
		err := decoder.Decode(&request)

		checkError(err)
		sendReply(request.Id, FindPrimes(request.Interval), encoder)
	}
}

func main() {

	CONN_TYPE := "tcp"
	CONN_HOST := "155.210.154.205"
	CONN_PORT := "30000"

	fmt.Println("Esperando clientes")
	for {
		listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
		checkError(err)

		conn, err := listener.Accept()
		defer conn.Close()
		checkError(err)

		// TO DO
		fmt.Println("Cliente conectado")

		encoder := gob.NewEncoder(conn)
		decoder := gob.NewDecoder(conn)

		fmt.Println("Creado encoder y decoder")

		receiveRequest(decoder, encoder)
	}
}
