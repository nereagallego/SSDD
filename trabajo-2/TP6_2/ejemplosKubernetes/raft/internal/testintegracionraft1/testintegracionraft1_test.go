package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"
	"raft/internal/maquinaestados"

	//"log"
	//"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	MAQUINA1 = "155.210.154.198"
	MAQUINA2 = "155.210.154.196"
	MAQUINA3 = "155.210.154.197"

	//puerto6
	PUERTOREPLICA1 = "29597"
	PUERTOREPLICA2 = "29597"
	PUERTOREPLICA3 = "29597"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_ed25519"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var PATH string = filepath.Join(os.Getenv("HOME"), "tmp", "p5", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// TEST primer rango
//func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
// <setup code>
// Crear canal de resultados de ejecuciones ssh en maquinas remotas
//	cfg := makeCfgDespliegue(t,
//		3,
//		[]string{REPLICA1, REPLICA2, REPLICA3},
//		[]bool{true, true, true})

// tear down code
// eliminar procesos en máquinas remotas
//	defer cfg.stop()

// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
//	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
//		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

//	t.Run("T5:SinAcuerdoPorFallos ",
//		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

//	t.Run("T5:SometerConcurrentementeOperaciones ",
//		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

//}

func TestPrimero(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T1:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

}

func TestSegundo(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T2:NoAcuerdoDesconexionesDeSeguidores ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

}

func TestTercero(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T3:OperacionesConcurrentes",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	time.Sleep(2500 * time.Millisecond)

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()
	time.Sleep(2500 * time.Millisecond)
	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	// Desconectar lider
	// ???
	cfg.stopLider(3)
	time.Sleep(5500 * time.Millisecond)
	//	cfg.stopLider(3)

	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	// A completar ???
	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	time.Sleep(6000 * time.Millisecond)
	//	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	idLider := cfg.leader(3)

	time.Sleep(4 * time.Second)
	var op maquinaestados.TipoOperacion
	op.Operacion = "escribir"
	op.Clave = "holita"
	op.Valor = "caracolita"
	cfg.comprometerOperacion(idLider, op, "escrito")

	op.Operacion = "leer"
	op.Clave = "holita"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "caracolita")

	op.Operacion = "leer"
	op.Clave = "holit"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "")

}

func (cfg *configDespliegue) leader(numreplicas int) int {
	//var reply raft.Vacio
	for i := 0; i < numreplicas; i++ {
		_, _, c, _ := cfg.obtenerEstadoRemoto(i)
		if c {
			return i
		}
	}
	return -1

}

func (cfg *configDespliegue) comprometerOperacion(replica int, op maquinaestados.TipoOperacion, resultadoDeseado string) {
	fmt.Println("Intento comprometer en la replica ", replica, " la entrada ", op.Operacion, " ", op.Clave, " ", op.Valor)
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[replica].CallTimeout("NodoRaft.SometerOperacionRaft", &op, &reply, 2000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")

	if reply.IndiceRegistro == -1 || reply.ValorADevolver != resultadoDeseado || !reply.EsLider {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s, resultado obtenido: %s, resultado deseado: %s",
			replica, cfg.t.Name(), reply.ValorADevolver, resultadoDeseado)
	}

}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	// A completar ???

	// Comprometer una entrada
	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	time.Sleep(6000 * time.Millisecond)
	//	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	cfg.desconectaSeguidor(3)

	idLider := cfg.leader(3)

	time.Sleep(4 * time.Second)
	var op maquinaestados.TipoOperacion
	op.Operacion = "escribir"
	op.Clave = "holita"
	op.Valor = "caracolita"
	cfg.comprometerOperacion(idLider, op, "escrito")

	op.Operacion = "leer"
	op.Clave = "holita"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "caracolita")

	op.Operacion = "leer"
	op.Clave = "holit"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "")

	// Comprobar varios acuerdos con una réplica desconectada

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
}

func (cfg *configDespliegue) desconectaSeguidor(numreplicas int) {
	var reply raft.Vacio
	for i := 0; i < numreplicas; i++ {
		_, _, c, _ := cfg.obtenerEstadoRemoto(i)
		if !c {
			err := cfg.nodosRaft[i].CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			cfg.t.Log("Paro la replica ", i)
			check.CheckError(err, "Error en llamada RPC Para nodo")
			break
		}
	}
}

func (cfg *configDespliegue) NOcomprometerOperacion(replica int, op maquinaestados.TipoOperacion, resultadoDeseado string) {
	fmt.Println("Intento comprometer en la replica ", replica, " la entrada ", op.Operacion, " ", op.Clave, " ", op.Valor)
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[replica].CallTimeout("NodoRaft.SometerOperacionRaft", &op, &reply, 2000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")

	if reply.IndiceRegistro == -1 || reply.ValorADevolver != resultadoDeseado || !reply.EsLider {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			replica, cfg.t.Name())
	}

}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	t.Skip("SKIPPED SinAcuerdoPorFallos")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	time.Sleep(5000 * time.Millisecond)
	//	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	idLider := cfg.desconectaSeguidores(3)

	time.Sleep(4 * time.Second)
	var op maquinaestados.TipoOperacion
	op.Operacion = "escribir"
	op.Clave = "holita"
	op.Valor = "caracolita"
	cfg.comprometerOperacion(idLider, op, "error")

	op.Operacion = "leer"
	op.Clave = "holita"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "error")

	op.Operacion = "leer"
	op.Clave = "holit"
	op.Valor = ""
	cfg.comprometerOperacion(idLider, op, "error")
}

func (cfg *configDespliegue) desconectaSeguidores(numreplicas int) int {
	var reply raft.Vacio
	lider := -1
	for i := 0; i < numreplicas; i++ {
		_, _, c, _ := cfg.obtenerEstadoRemoto(i)
		if !c {
			cfg.t.Log("Paro la replica ", i)
			err := cfg.nodosRaft[i].CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		} else {
			lider = i
		}
	}
	return lider
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	// A completar ???
	//	t.Skip("SKIPPED SinAcuerdoPorFallos")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft
	time.Sleep(5000 * time.Millisecond)
	//	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)

	idLider := cfg.leader(3)

	var op maquinaestados.TipoOperacion
	op.Operacion = "leer"
	op.Clave = "holita"
	op.Valor = ""

	for i := 0; i < 5; i++ {
		cfg.comprometerOperacionConc(idLider, op, i)
	}

	cfg.comprobarMandato(3)
	// un bucle para estabilizar la ejecucion

	// Obtener un lider y, a continuación someter una operacion

	// Someter 5  operaciones concurrentes

	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
}

func (cfg *configDespliegue) comprometerOperacionConc(replica int, op maquinaestados.TipoOperacion, resultadoDeseado int) {
	fmt.Println("Intento comprometer en la replica ", replica, " la entrada ", op.Operacion, " ", op.Clave, " ", op.Valor)
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[replica].CallTimeout("NodoRaft.SometerOperacionRaft", &op, &reply, 5000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")

	if reply.IndiceRegistro != resultadoDeseado || !reply.EsLider {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s, resultado obtenido: %d, resultado deseado: %d",
			replica, cfg.t.Name(), reply.IndiceRegistro, resultadoDeseado)
	}

}

func (cfg *configDespliegue) comprobarMandato(numreplicas int) {
	for i := 0; i+1 < numreplicas; i++ {
		_, a, _, _ := cfg.obtenerEstadoRemoto(i)
		_, b, _, _ := cfg.obtenerEstadoRemoto(i + 1)
		if a != b {
			cfg.t.Fatalf("Estado incorrecto en replica %d o  en la replica %d en subtest %s", i, i+1, cfg.t.Name())
		}
	}
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		//time.Sleep(500 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(8000 * time.Millisecond)
}

func (cfg *configDespliegue) stopLider(numreplicas int) {
	lider := -1
	var reply raft.Vacio
	for i := 0; i < numreplicas; i++ {
		_, _, c, _ := cfg.obtenerEstadoRemoto(i)
		if c {
			lider = i
			cfg.t.Log("Kill leader ", i)
			err := cfg.nodosRaft[i].CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}
	despliegue.ExecMutipleHosts(EXECREPLICACMD+" "+strconv.Itoa(lider)+" "+rpctimeout.HostPortArrayToString(cfg.nodosRaft), []string{cfg.nodosRaft[lider].Host()}, cfg.cr, PRIVKEYFILE)
}

//
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for _, endPoint := range cfg.nodosRaft {
		err := endPoint.CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 10*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	cfg.t.Log("Estado replica ", idNodoDeseado, " : ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		for s := range cfg.cr {
			fmt.Println(s)
		}
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())

	}

}
