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
	"math/rand"
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
	//	chLider      chan (string)
}

func New(id int, coordinador int, ficheroMaton string) *Maton { //, chLider *chan string) *Maton {
	messageTypes := []ms.Message{OK{}, ELECTION{}, COORDINATOR{}, LEADERBEAT{}}
	msgs := ms.New(id, ficheroMaton, messageTypes)
	maton := Maton{id, msgs.NumberOfPeers(), coordinador, &msgs, make(chan string), make(chan string)} //, *chLider}
	go maton.Receive()
	if !maton.soyLider() {
		go maton.Send()
	} //else {
	//		*chLider <- "ok"
	//	}
	return &maton
}

func (m *Maton) Send() {
	for {
		time.Sleep(time.Duration(rand.Intn(20-10)+10) * time.Second)
		if !m.soyLider() {
			m.ms.Send(m.Coordinador, LEADERBEAT{m.Id})
			go m.ReceiveLeaderbeat()
		}
	}
}
func (m *Maton) soyLider() bool {
	return m.Id == m.Coordinador
}

func (m *Maton) NuevoCoordinador() {
	for j := 1; j <= m.N; j++ {
		if j != m.Id {
			m.ms.Send(m.Id, COORDINATOR{m.Id})
		}
	}
}

func (m *Maton) NuevaEleccion() {
	for j := m.Id + 1; j <= m.N; j++ {
		m.ms.Send(m.Id, ELECTION{m.Id})
	}
	m.ReceiveOk()
}

func (m *Maton) ReceiveOk() {
	select {
	case <-m.chOk:
		break
	case <-time.After(10 * time.Second):
		m.NuevoCoordinador()
	}
}

func (m *Maton) ReceiveLeaderbeat() {
	select {
	case <-m.chLeaderBeat:
		break
	case <-time.After(10 * time.Second):
		m.NuevaEleccion()
	}
}

func (m *Maton) Receive() {
	for {
		msg := m.ms.Receive()
		switch v := msg.(type) {
		case OK:
			m.chOk <- "ok"
		case COORDINATOR:
			m.Coordinador = v.Id
			//	m.chLider <- "ok"
			go m.Send()
		case ELECTION:
			if m.Id > v.Id {
				m.ms.Send(v.Id, OK{})
			}
		case LEADERBEAT:
			if m.soyLider() {
				comp := rand.Intn(100-2) + 2
				if (comp % 13) == 0 {
					time.Sleep(50 * time.Second)
				}
				m.ms.Send(v.Id, LEADERBEAT{m.Id})
			} else {
				m.chLeaderBeat <- "ok"
			}
		}
	}
}
