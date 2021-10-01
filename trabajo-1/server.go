/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes al trabajo 1
 */
package main

import (
	"fmt"
	"net"
	"os"
	"trabajo-1/com"
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

func desconvertirServ(v []byte) com.TPInterval {
	var intv com.TPInterval
	intv.A = 0
	intv.B = 0
	peso := 1
	for i := 0; i < len(v); i = i + 2 {
		intv.A += int(v[i]) * peso
		intv.B += int(v[i+1]) * peso
		peso *= 256
	}

	return intv
}

func convertirServ(primes []int) []byte {
	var v []byte
	for i := 0; i < len(primes); i = i + 1 {
		for j := 0; j < 4; j = j + 1 {
			v = append(v, byte(primes[i]%256))
			primes[i] = primes[i] / 256
		}
	}
	return v
}

func main() {

	CONN_TYPE := "tcp"
	CONN_HOST := "155.210.154.200"
	CONN_PORT := "30000"

	fmt.Println("Esperando clientes...")

	listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	checkError(err)

	conn, err := listener.Accept()
	defer conn.Close()
	checkError(err)

	//var v [8]byte
	v := make([]byte, 8)
	_, err = conn.Read(v)
	checkError(err)

	fmt.Println("Deserializando los datos...")
	fmt.Println("Buscando los primos...")
	fmt.Println("Enviado los primos...")

	_, err = conn.Write(convertirServ(FindPrimes(desconvertirServ(v))))
	checkError(err)

	conn.Close()

}
