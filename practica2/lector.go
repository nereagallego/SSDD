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
	typeOfProcess := 0 //lector
	ra := ra.New(pid, usersFile, typeOfProcess)
	gestorfichero := gestorfichero.NewGestor("../"+file, pid, "../ms/"+usersFile)
	ra.PreProtocol()

	//SC
	texto := gestorfichero.LeerFichero()
	fmt.Println(texto)

	ra.PostProtocol()
}
