/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: client.go
* DESCRIPCIÓN: cliente completo para los cuatro escenarios de la práctica 1
 */
package main

import (
	"fmt"
	"io"
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

func convertirCli(intv com.TPInterval) []byte {
	var v []byte
	for i := 0; i < 4; i = i + 1 {
		v = append(v, byte(intv.A%256))
		v = append(v, byte(intv.B%256))
		intv.A = intv.A / 256
		intv.B = intv.B / 256
	}
	return v
}

func desconvertirCli(v []byte) []int {
	var primes []int
	for i := 0; i < len(v); i = i + 4 {
		aux := 0
		peso := 1
		for j := 0; j < 4; j = j + 1 {
			aux += int(v[i+j]) * peso
			peso *= 256
		}
		primes = append(primes, aux)
	}

	return primes
}

func main() {
	endpoint := "155.210.154.200:30000"

	fmt.Println("Introduce un intervalo de enteros: ")

	// TODO: crear el intervalo solicitando dos números por teclado
	i1 := 7
	i2 := 5

	for i1 > i2 {
		fmt.Scanln(&i1, &i2)
	}

	interval := com.TPInterval{i1, i2}
	fmt.Println("Serializando los datos...")
	vect := convertirCli(interval)

	tcpAddr, err := net.ResolveTCPAddr("tcp", endpoint)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	_, err = conn.Write(vect)
	checkError(err)

	fmt.Println("Esperando respuesta...")

	p := make([]byte, 4)
	_, err = conn.Read(p)
	checkError(err)
	var v []byte

	fmt.Println("Deserializando los números...")

	for err != io.EOF {
		v = append(v, p[0])
		v = append(v, p[1])
		v = append(v, p[2])
		v = append(v, p[3])
		_, err = conn.Read(p)

	}
	fmt.Println("Los números primos son: ", desconvertirCli(v))

	// la variable conn es de tipo *net.TCPconn
}
