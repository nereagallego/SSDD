/*
* AUTOR: Cesar Borja Moreno, Nerea Gallego Sánchez
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: escritor.go
* DESCRIPCIÓN: Implementación del escritor en Go
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
	file := args[4]

	typeOfProcess := 1 //escritor

	logger := govec.InitGoVector("escritor"+strconv.Itoa(pid), "Escritor"+strconv.Itoa(pid)+"File", govec.GetDefaultConfig())

	fragmento := args[3]

	gestorfichero := gestorfichero.NewGestor(file)
	//ms := ms.New(pid, usersFile, []ms.Message{ra.Request{}, ra.Reply{}, ra.Escribir{}})
	ra := ra.New(pid, usersFile, typeOfProcess, logger, gestorfichero)
	time.Sleep(5000 * time.Millisecond)
	for i := 0; i < 10; i++ {
		ra.PreProtocol()

		//SC

		logger.LogLocalEvent("escribe "+fragmento, govec.GetDefaultLogOptions())
		gestorfichero.EscribirFichero(fragmento)
		ra.SendEscribir(fragmento)
		ra.PostProtocol()
		time.Sleep(2000 * time.Millisecond)
	}
	time.Sleep(5000 * time.Millisecond)
}
