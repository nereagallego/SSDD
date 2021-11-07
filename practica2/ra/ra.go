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
	"fmt"
	"practica2/gestorfichero"
	"practica2/ms"
	"sync"

	"github.com/DistributedClocks/GoVector/govec"
)

type Escribir struct {
	Fragmento          string
	VectorClockMessage []byte
}

type Request struct {
	SequenceNumber     int
	Pid                int
	OperationType      int
	VectorClockMessage []byte
}

type Reply struct {
	VectorClockMessage []byte
}

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
	TypeOfProcess int        // 0 si es lector y 1 si el proceso es escritor
	logger        *govec.GoLog
	gestor        *gestorfichero.Gestor
}

var exclude = [2][2]bool{
	{false, true},
	{true, true}}

func New(me int, usersFile string, typeOfProcess int, logger *govec.GoLog, gestor gestorfichero.Gestor) *RASharedDB {

	messageTypes := []ms.Message{Escribir{}, Request{}, Reply{}}
	msgs := ms.New(me, usersFile, messageTypes)
	ra := RASharedDB{0, 0, 0, false, []bool{}, &msgs, make(chan bool), make(chan bool), sync.Mutex{}, msgs.NumberOfPeers(), typeOfProcess, logger, &gestor}
	go ra.Receive()
	for i := 0; i < ra.N; i++ {
		ra.RepDefd = append(ra.RepDefd, false)
	}
	fmt.Println(ra.N)
	return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	ra.Mutex.Lock()
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.Mutex.Unlock()
	ra.OutRepCnt = ra.N - 1
	for j := 1; j <= ra.N; j++ { //enviar request a todos menos a ti mismo
		if j != ra.ms.Me() {
			VectorClockMessage := ra.logger.PrepareSend("Sending Request", "request", govec.GetDefaultLogOptions())

			ra.ms.Send(j, Request{ra.OurSeqNum, ra.ms.Me(), ra.TypeOfProcess, VectorClockMessage})
		}
	}
	for ra.OutRepCnt > 0 { //recibir todas las replys
		_ = <-ra.chrep
		ra.OutRepCnt = ra.OutRepCnt - 1
	}
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	ra.Mutex.Lock()
	ra.ReqCS = false

	for j := 1; j <= ra.N; j++ {
		if ra.RepDefd[j-1] {
			ra.RepDefd[j-1] = false
			VectorClockMessage := ra.logger.PrepareSend("Sending Reply", "reply", govec.GetDefaultLogOptions())

			ra.ms.Send(j, Reply{VectorClockMessage})
		}
	}
	ra.Mutex.Unlock()
}

func (ra *RASharedDB) Receive() {
	ra.logger.LogLocalEvent("Receiver active ", govec.GetDefaultLogOptions())
	for {
		mensaje := ra.ms.Receive()
		var incomming string

		switch v := mensaje.(type) {
		case Reply:
			ra.logger.UnpackReceive("Received reply", v.VectorClockMessage, &incomming, govec.GetDefaultLogOptions())
			ra.permision()

		case Request:
			ra.logger.UnpackReceive("Received Request", v.VectorClockMessage, &incomming, govec.GetDefaultLogOptions())
			ra.Request(v)
		case Escribir:
			ra.logger.UnpackReceive("Received escribir en el fichero "+v.Fragmento, v.VectorClockMessage, &incomming, govec.GetDefaultLogOptions())
			ra.gestor.EscribirFichero(v.Fragmento)
		default:
			fmt.Println("ERROR!!!!")
		}
	}
}

func (ra *RASharedDB) Request(mensaje Request) {
	ra.Mutex.Lock()
	ra.HigSeqNum = Max(ra.HigSeqNum, mensaje.SequenceNumber)
	defer_p := ra.ReqCS && (ra.OurSeqNum < mensaje.SequenceNumber || (ra.OurSeqNum == mensaje.SequenceNumber && mensaje.Pid > ra.ms.Me()))
	ra.Mutex.Unlock()
	if defer_p && exclude[ra.TypeOfProcess][mensaje.OperationType] {
		ra.logger.LogLocalEvent("defer", govec.GetDefaultLogOptions())
		ra.Mutex.Lock()
		ra.RepDefd[mensaje.Pid-1] = true
		ra.Mutex.Unlock()
	} else {
		VectorClockMessage := ra.logger.PrepareSend("Sending Reply", "send reply", govec.GetDefaultLogOptions())

		ra.ms.Send(mensaje.Pid, Reply{VectorClockMessage})
	}

}

func (ra *RASharedDB) permision() {
	ra.chrep <- true
}

func (ra *RASharedDB) SendEscribir(f string) {
	ra.logger.LogLocalEvent("Indicar a resto de procesos que deben escribir", govec.GetDefaultLogOptions())
	for j := 1; j <= ra.N; j++ {
		if j != ra.ms.Me() {
			VectorClockMessage := ra.logger.PrepareSend("Sending Escribir", "escribe "+f, govec.GetDefaultLogOptions())

			ra.ms.Send(j, Escribir{f, VectorClockMessage})
		}
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (ra *RASharedDB) Stop() {
	ra.ms.Stop()
	ra.done <- true
}
