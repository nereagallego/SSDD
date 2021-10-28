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
	"os"
	"practica2/gestorfichero"
	"practica2/ra"
	"strconv"
)

func main() {
	args := os.Args
	pid, err := strconv.Atoi(args[1])
	if err != nil {
		checkError(err)
	}
	usersFile := args[2]
	file := args[3]
	typeOfProcess := 1 //escritor
	ra := ra.New(pid, usersFile, typeOfProcess)
	ra.PreProtocol()

	//SC
	gestor := gestorfichero.NewGestor("../"+file, pid, "../ms/"+usersFile)
	fragmento := "escritura" + args[1] + "\n"
	gestor.EscribirFichero(fragmento)

	ra.PostProtocol()
}
