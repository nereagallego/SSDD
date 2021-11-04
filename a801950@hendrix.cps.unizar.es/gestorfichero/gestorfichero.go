/*
* AUTOR: Cesar Borja Moreno, Nerea Gallego Sánchez
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: gestorfichero.go
* DESCRIPCIÓN: Implementación del gestor de fichero en Go
 */
package gestorfichero

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Gestor struct {
	file string
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func (g *Gestor) LeerFichero() string {
	datosComoB, err := ioutil.ReadFile(g.file)
	checkError(err)

	return string(datosComoB)
}

func (g *Gestor) EscribirFichero(fragmento string) {
	texto := g.LeerFichero()
	texto = texto + fragmento + "\n"
	err := ioutil.WriteFile(g.file, []byte(texto), 0644)
	checkError(err)
}

func NewGestor(filename string) (g Gestor) {
	g.file = filename
	return g
}
