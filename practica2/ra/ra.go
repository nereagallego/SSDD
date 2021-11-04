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
	Fragmento string
}

type Request struct {
	SequenceNumber int
	Pid            int
	OperationType  int
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
	TypeOfProcess int        // 0 si es lector y 1 si el proceso es escritor
	logger        *govec.GoLog
	gestor        *gestorfichero.Gestor
	// TODO: completar
}

var exclude = [2][2]bool{
	{false, true},
	{true, true}}

func New(me int, usersFile string, typeOfProcess int, logger *govec.GoLog, gestor gestorfichero.Gestor) *RASharedDB {

	messageTypes := []ms.Message{Escribir{}, Request{}, Reply{}}
	//messageTypes := []ms.Message{[]byte{}}
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
	ra.Mutex.Lock()
	for j := 1; j <= ra.N; j++ { //enviar request a todos menos a ti mismo
		if j != ra.ms.Me() {
			_ = ra.logger.PrepareSend("Sending Request", Request{ra.OurSeqNum, ra.ms.Me(), ra.TypeOfProcess}, govec.GetDefaultLogOptions())
			ra.ms.Send(j, Request{ra.OurSeqNum, ra.ms.Me(), ra.TypeOfProcess})
		}
	}
	ra.Mutex.Unlock()
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
			_ = ra.logger.PrepareSend("Sending Message", Reply{}, govec.GetDefaultLogOptions())
			ra.ms.Send(j, Reply{})
		}
	}
	ra.Mutex.Unlock()
}

func (ra *RASharedDB) Receive() {
	ra.logger.LogLocalEvent("receiver active ", govec.GetDefaultLogOptions())
	for {
		mensaje := ra.ms.Receive()
		var vectorClockMessage []byte

		switch v := mensaje.(type) {
		case Reply:
			ra.permision()
			ra.logger.UnpackReceive("Receive reply", vectorClockMessage, &mensaje, govec.GetDefaultLogOptions())
		case Request:
			ra.logger.UnpackReceive("Receiving Request", vectorClockMessage, &mensaje, govec.GetDefaultLogOptions())
			ra.Request(v)
		case Escribir:
			ra.logger.UnpackReceive("Receiving escribir en el fichero "+v.Fragmento, vectorClockMessage, &mensaje, govec.GetDefaultLogOptions())
			ra.gestor.EscribirFichero(v.Fragmento)
		default:
			ra.logger.UnpackReceive("Receiving incorrect type ", vectorClockMessage, &mensaje, govec.GetDefaultLogOptions())
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
		ra.logger.LogLocalEvent("defer ", govec.GetDefaultLogOptions())
		ra.Mutex.Lock()
		ra.RepDefd[mensaje.Pid-1] = true
		ra.Mutex.Unlock()
	} else {
		_ = ra.logger.PrepareSend("Sending Reply", Reply{}, govec.GetDefaultLogOptions())
		ra.ms.Send(mensaje.Pid, Reply{})
	}

}

func (ra *RASharedDB) permision() {
	ra.chrep <- true
}

func (ra *RASharedDB) SendEscribir(f string) {
	ra.logger.LogLocalEvent("Indicar a resto de procesos que deben escribir", govec.GetDefaultLogOptions())
	for j := 1; j <= ra.N; j++ {
		if j != ra.ms.Me() {
			_ = ra.logger.PrepareSend("Sending Escribir", Escribir{f}, govec.GetDefaultLogOptions())
			ra.ms.Send(j, Escribir{f})
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
