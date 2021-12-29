package maquinaestados

//
// API
// ===
//

import (
	"sync"
)

type MaquinaEstados struct {
	Mux   sync.Mutex // Mutex para proteger acceso a estado compartido
	Datos map[string]string
}

func NuevaMaquinaEstados() *MaquinaEstados {
	me := &MaquinaEstados{sync.Mutex{}, map[string]string{}}
	return me
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
