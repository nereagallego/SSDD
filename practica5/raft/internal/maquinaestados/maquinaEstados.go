package maquinaestados

//
// API
// ===
//

import (
	"fmt"
	"sync"
)

type MaquinaEstados struct {
	Mux            sync.Mutex // Mutex para proteger acceso a estado compartido
	Datos          map[string]string
	CanalAplicar   chan AplicaOperacion
	CanalRespuesta chan string
}

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Mandato   int // en la entrada de registro // mandato
	Operacion TipoOperacion
}

func NuevaMaquinaEstados(canalAplicar chan AplicaOperacion, canalRespuesta chan string) {
	me := &MaquinaEstados{sync.Mutex{}, map[string]string{}, canalAplicar, canalRespuesta}
	go me.Receive()

}

func (me *MaquinaEstados) Receive() {
	for {
		op := <-me.CanalAplicar
		if op.Operacion.Operacion == "leer" {
			fmt.Println("leo")
			me.CanalRespuesta <- me.Leer(op.Operacion.Clave)
		} else if op.Operacion.Operacion == "escribir" {
			me.Escribir(op.Operacion.Clave, op.Operacion.Valor)
			me.CanalRespuesta <- ""
		}
	}
}

func (me *MaquinaEstados) Escribir(clave string, valor string) {
	me.Mux.Lock()
	me.Datos[clave] = valor
	me.Mux.Unlock()
}

func (me *MaquinaEstados) Leer(clave string) string {
	me.Mux.Lock()
	s := me.Datos[clave]
	me.Mux.Unlock()
	return s
}
