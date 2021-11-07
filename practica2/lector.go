/*
* AUTOR: Cesar Borja Moreno, Nerea Gallego Sánchez
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: lector.go
* DESCRIPCIÓN: Implementación del lector en Go
 */
package main

import (
	"fmt"
	"os"
	"practica2/gestorfichero"
	"practica2/ra"
	"strconv"
	"time"

	"github.com/DistributedClocks/GoVector/govec"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {
	args := os.Args
	pid, err := strconv.Atoi(args[1])
	if err != nil {
		checkError(err)
	}
	usersFile := args[2]
	file := args[3]

	logger := govec.InitGoVector("lector"+strconv.Itoa(pid), "Lector"+strconv.Itoa(pid)+"File", govec.GetDefaultConfig())

	typeOfProcess := 0 //lector

	gestorfichero := gestorfichero.NewGestor(file)
	ra := ra.New(pid, usersFile, typeOfProcess, logger, gestorfichero)
	time.Sleep(5000 * time.Millisecond)
	for i := 0; i < 10; i++ {
		ra.PreProtocol()

		//SC
		logger.LogLocalEvent("leer fichero ", govec.GetDefaultLogOptions())
		texto := gestorfichero.LeerFichero()
		fmt.Println(texto)

		ra.PostProtocol()
		time.Sleep(2000 * time.Millisecond)
	}
	time.Sleep(5000 * time.Millisecond)
}
