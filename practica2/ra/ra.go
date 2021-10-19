/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 */
package ra

import (
	"practica2/ms"
	"sync"
)

type Request struct {
	Clock int
	Pid   int
}

type Reply struct{}

type RASharedDB struct {
	OurSeqNum int
	HigSeqNum int
	OutRepCnt int
	ReqCS     bool
	RepDefd   []bool
	ms        *MessageSystem
	done      chan bool
	chrep     chan bool
	Mutex     sync.Mutex // mutex para proteger concurrencia sobre las variables
	N         int        // número de procesos
	// TODO: completar
}

func New(me int, usersFile string) *RASharedDB {
	messageTypes := []Message{Request, Reply}
	msgs = ms.New(me, usersFile, messageTypes)
	ra := RASharedDB{0, 0, 0, false, []int{}, &msgs, make(chan bool), make(chan bool), &sync.Mutex{}}
	// TODO completar
	return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	// TODO completar
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.OutRepCnt = ra.N - 1
	for j := 1; j <= ra.N; j++ {
		if j != ra.ms.me {
			ra.ms.Send(j, Request{ra.OurSeqNum, ra.ms.me})
		}
	}
	for ra.OutRepCnt > 0 {
		_ = ra.ms.Receive()
		ra.OutRepCnt = ra.OutRepCnt - 1
	}
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	// TODO completar
	ra.ReqCS = false
	for j := 1; j <= ra.N; j++ {
		if ra.RepDefd[j] {
			ra.RepDefd[j] = false
			ra.ms.Send(j, Reply{})
		}
	}

}

func (ra *RASharedDB) Stop() {
	ra.ms.Stop()
	ra.done <- true
}
