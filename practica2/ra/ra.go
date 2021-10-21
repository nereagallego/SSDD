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
	Clock         int
	Pid           int
	OperationType int
}

type Reply struct{}

type RASharedDB struct {
	OurSeqNum     int
	HigSeqNum     int
	OutRepCnt     int
	ReqCS         bool
	RepDefd       []bool
	ms            *ms.MessageSystem
	done          chan bool
	chrep         chan bool
	Mutex         sync.Mutex // mutex para proteger concurrencia sobre las variables
	N             int        // número de procesos
	typeOfProcess int        // 0 si es lector y 1 si el proceso es escritor
	// TODO: completar
}

var exclude = [2][2]bool{
	{false, true},
	{true, true}}

func New(me int, usersFile string, typeOfProcess int) *RASharedDB {

	messageTypes := []ms.Message{Request{}, Reply{}}
	msgs := ms.New(me, usersFile, messageTypes)
	ra := RASharedDB{0, 0, 0, false, []bool{}, &msgs, make(chan bool), make(chan bool), sync.Mutex{}, msgs.NumberOfPeers(), typeOfProcess}
	// TODO completar
	return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	// TODO completar
	ra.Mutex.Lock()
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.Mutex.Unlock()
	ra.OutRepCnt = ra.N - 1
	for j := 1; j <= ra.N; j++ { //enviar request a todos menos a ti mismo
		if j != ra.ms.Me() {
			ra.ms.Send(j, Request{ra.OurSeqNum, ra.ms.Me(), ra.typeOfProcess})
		}
	}
	for ra.OutRepCnt > 0 { //recibir todas las replys
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

func (ra *RASharedDB) Receive(){
	for {
		mensaje := ra.ms.Receive()
		if mensaje == Reply{} {
			ra.permision()
		} else {
			ra.Request(mensaje)
		}
		
	}
}

func (ra *RASharedDB) Request(menmensaje ms.Message){
	for {
		

	}
}

func (ra *RASharedDB) Stop() {
	ra.ms.Stop()
	ra.done <- true
}
