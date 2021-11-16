/*
* AUTOR: Nerea Gallego Sánchez, César Borja Moreno
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: 16 de noviembre de 2021
* FICHERO: maton.go
* DESCRIPCIÓN: contiene el algoritmo del maton de la practica 3
 */
package maton

import (
	"practica3/ms"
	"time"
)

type OK struct {
	Id int
}

type ELECTION struct {
	Id int
}

type COORDINATOR struct {
	Id int
}

type LEADERBEAT struct {
	Id int
}

type Maton struct {
	Id           int
	N            int
	Coordinador  int
	ms           *ms.MessageSystem
	chOk         chan (string)
	chLeaderBeat chan (string)
}

func (maton *Maton) soyLider() bool {
	return maton.Id == maton.Coordinador
}

func New(id int, coordinador int, ficheroMaton string) *Maton {
	messageTypes := []ms.Message{OK{}, ELECTION{}, COORDINATOR{}, LEADERBEAT{}}
	msgs := ms.New(id, ficheroMaton, messageTypes)
	maton := Maton{id, msgs.NumberOfPeers(), coordinador, &msgs, make(chan string), make(chan string)}
	go maton.Receive()
	return &maton
}

func (maton *Maton) Send() {
	for {
		time.Sleep(10 * time.Second)
		if !maton.soyLider() {
			maton.ms.Send(maton.Coordinador, LEADERBEAT{maton.Id})
		}
		go maton.ReceiveLeaderbeat()
	}
}

func (maton *Maton) NuevoCoordinador() {
	for j := 1; j <= maton.N; j++ {
		if j != maton.Id {
			maton.ms.Send(maton.Id, COORDINATOR{maton.Id})
		}
	}
}

func (maton *Maton) NuevaEleccion() {
	for j := maton.Id + 1; j <= maton.N; j++ {
		maton.ms.Send(maton.Id, ELECTION{maton.Id})
	}
	maton.ReceiveOk()
}

func (maton *Maton) ReceiveOk() {
	select {
	case <-maton.chOk:
		break
	case <-time.After(10 * time.Second):
		maton.NuevoCoordinador()
	}
}

func (maton *Maton) ReceiveLeaderbeat() {
	select {
	case <-maton.chLeaderBeat:
		break
	case <-time.After(10 * time.Second):
		maton.NuevaEleccion()
	}
}

func (maton *Maton) Receive() {
	for {
		msg := maton.ms.Receive()
		switch msg.(type) {
		case OK:
			maton.chOk <- "ok"
		case COORDINATOR:
			maton.Coordinador = msg.(COORDINATOR).Id
		case ELECTION:
			if maton.Id > msg.(ELECTION).Id {
				maton.ms.Send(msg.(ELECTION).Id, OK{})
			}
		case LEADERBEAT:
			if maton.soyLider() {
				maton.ms.Send(msg.(LEADERBEAT).Id, LEADERBEAT{maton.Id})
			} else {
				maton.chLeaderBeat <- "ok"
			}
		}
	}
}
