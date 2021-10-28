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
	"practica2/ms"
)

type mensaje struct {
	m string
}

type gestorfichero struct {
	file string
	ms   ms.MessageSystem
	N    int
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func (g *gestorfichero) LeerFichero() string {
	datosComoB, err := ioutil.ReadFile(g.file)
	checkError(err)

	return string(datosComoB)
}

func (g *gestorfichero) EscribirFichero(fragmento string) {
	texto := g.LeerFichero()
	texto = texto + fragmento
	err := ioutil.WriteFile(g.file, []byte(texto), 0644)
	checkError(err)
	for i := 1; i <= g.N; i++ {
		if i != g.ms.Me() {
			g.ms.Send(i, mensaje{fragmento})
		}
	}
}

func NewGestor(filename string, whoIam int, usersFile string) (g gestorfichero) {
	g.file = filename
	messageTypes := []ms.Message{mensaje{}}
	g.ms = ms.New(whoIam, usersFile, messageTypes)
	g.N = g.ms.NumberOfPeers()
	go g.Receive()
	return g
}

func (g *gestorfichero) Receive() {
	for {
		mensaje := g.ms.Receive()
		g.EscribirFichero(mensaje.(string))
	}
}
