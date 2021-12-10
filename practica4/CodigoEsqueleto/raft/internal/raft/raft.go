// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"raft/internal/comun/rpctimeout"
	"sync"
	"time"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = true

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = true

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "./logs_raft/"

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro // mandato
	Operacion interface{}
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Vuestros datos aqui.
	CurrentTerm int // Ultimo mandato que ha visto
	VotedFor    int // Candidato que he votado (null si nadi)
	Logs        []AplicaOperacion

	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int
	Latidos     chan bool

	Hevotado bool
	Voto     chan bool
	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft
}

var timeMaxLatido int = 1000
var TIME_LATIDO int = 1000 / 18
var TIME_MSG int = 10
var TIME_VOTO int = 300

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicar chan AplicaOperacion) *NodoRaft {
	fmt.Println("Intento crear un nodo")
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.CurrentTerm = 0
	nr.IdLider = -1
	nr.LastApplied = 0
	nr.CommitIndex = 0
	nr.Latidos = make(chan bool)
	nr.Voto = make(chan bool)
	nr.Logs = []AplicaOperacion{}

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	go nr.gestionRaft()
	// Your initialization code here (2A, 2B)
	fmt.Println("Nodo creado")
	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var idLider int = nr.IdLider
	var esLider bool = (idLider == yo)

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(
	operacion interface{}) (int, int, bool, int) {
	indice := -1
	mandato := -1
	EsLider := true
	idLider := -1

	// Vuestro codigo aqui
	//	arg := AppendEntriesArgs{nr.CurrentTerm, nr.Yo, nr.CommitIndex, nr.Logs[nr.CommitIndex].Indice, []AplicaOperacion{AplicaOperacion{nr.CommitIndex + 1, operacion}}}
	prevLogIndex := -1
	prevLogTerm := -1
	if len(nr.Logs) > 0 {
		prevLogIndex = len(nr.Logs) - 1
		prevLogTerm = nr.Logs[len(nr.Logs)-1].Indice
	}
	out := nr.sendMsg([]AplicaOperacion{AplicaOperacion{nr.CurrentTerm, operacion}}, prevLogIndex, prevLogTerm)
	nr.Logs = append(nr.Logs, AplicaOperacion{nr.CurrentTerm, operacion})
	if out {
		indice = prevLogIndex + 1
		mandato = nr.CurrentTerm
		EsLider = nr.Yo == nr.IdLider
		idLider = nr.IdLider
		nr.CommitIndex++
	}
	return indice, mandato, EsLider, idLider
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion *interface{},
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato,
		reply.EsLider, reply.IdLider = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// ArgsPeticionVoto
// ===============
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RespuestaPeticionVoto
// ================
//
// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	Term        int
	VoteGranted bool
}

//
// PedirVoto
// ===========
//
// Metodo para RPC PedirVoto (me piden que les vote)
//
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	//	fmt.Println("Me han pedido que vote")
	reply.Term = nr.CurrentTerm
	reply.VoteGranted = args.Term >= nr.CurrentTerm
	//if &nr.VotedFor != nil && !reply.VoteGranted {
	if nr.Hevotado && !reply.VoteGranted {
		fmt.Println("Habia votado: ", nr.Hevotado)
		fmt.Println("My term: ", nr.CurrentTerm, " their term: ", args.Term)
		fmt.Println("No he votado a ", args.CandidateId)
		reply.VoteGranted = false
	} else {
		fmt.Println("He votado a ", args.CandidateId)
		nr.Hevotado = true
		nr.VotedFor = args.CandidateId
	}

	return nil
}

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petiión perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	out := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply, time.Duration(TIME_VOTO)*time.Millisecond)

	if out != nil {
		//	fmt.Println(out)
		return false
	}
	if reply.VoteGranted {
		nr.Voto <- true
	}
	return true
}

type Entrada interface{}

type AppendEntriesArgs struct {
	Term         int               // leader's term
	LeaderId     int               // leader ID
	PrevLogIndex int               // index of log entry immediaty preceding new ones
	PrevLogTerm  int               // term of prevLogIndex entry
	Entries      []AplicaOperacion // log entries to store (enpty for heartbeat)
	LeaderCommit int               // leader commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (nr *NodoRaft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	//	fmt.Println("llega append entries")
	if args.Term < nr.CurrentTerm {
		reply.Success = false
	} else if len(args.Entries) == 0 {
		//	fmt.Println("Llega un latido")
		if nr.CurrentTerm < args.Term {
			nr.CurrentTerm = args.Term
		}
		nr.IdLider = args.LeaderId
		nr.Latidos <- true
		reply.Success = true
	} else if nr.Logs[args.PrevLogIndex].Indice != args.PrevLogTerm {
		reply.Success = false
		//	}

	} else {
		for i := 0; i < len(args.Entries); i++ {
			nr.Logs = append(nr.Logs, args.Entries[i])
			nr.CommitIndex++
		}
		reply.Success = true
		reply.Term = nr.CurrentTerm
		//	if args.LeaderCommit > nr.CommitIndex {
		//	nr.CommitIndex = Min(args.LeaderCommit, nr.LastApplied)
		//	}
	}
	reply.Term = nr.CurrentTerm
	return nil
}

func (nr *NodoRaft) sendMsg(entradas []AplicaOperacion, prevLogIndex int, prevLogTerm int) bool {
	// tam := 0
	// if len(nr.Logs) > 0 {
	// 	tam = nr.Logs[len(nr.Logs)].Indice
	// }
	arg := AppendEntriesArgs{nr.CurrentTerm, nr.IdLider, prevLogIndex, prevLogTerm, entradas, nr.CommitIndex}
	var reply []AppendEntriesReply
	for i := 0; i < len(nr.Nodos); i++ {
		aux := AppendEntriesReply{0, true}
		reply = append(reply, aux)
	}
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			//		fmt.Println("Mando el latido a", i)
			go nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 1000*time.Millisecond)
			//	out := nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 1000*time.Millisecond)
			//	fmt.Println(out)
		}
	}
	time.Sleep(time.Duration(TIME_MSG) * time.Millisecond)
	count := 0
	for i := 0; i < len(nr.Nodos); i++ {
		if reply[i].Success {
			count++
		} else if reply[i].Term > nr.CurrentTerm {
			return false
		}
	}
	// if !(count > len(nr.Nodos)-count) && len(entradas) > 0 {
	// 	for count <= len(nr.Nodos)-count {
	// 		for i := 0; i < len(nr.Nodos); i++ {
	// 			if i != nr.Yo && !reply[i].Success {
	// 				go nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 1000*time.Millisecond)
	// 			}
	// 		}
	// 		time.Sleep(time.Duration(TIME_MSG) * time.Millisecond)
	// 		for i := 0; i < len(nr.Nodos); i++ {
	// 			if reply[i].Success {
	// 				count++
	// 			}
	// 		}
	// 	}

	// }
	return true
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (nr *NodoRaft) enviaLatidos() {
	soyLider := true
	fmt.Println("soy lider en el mandato ", nr.CurrentTerm)
	for soyLider {

		var e []AplicaOperacion
		out := nr.sendMsg(e, 0, 0)
		//fmt.Println("Envio latido")
		if !out {
			soyLider = false
		} else {
			time.Sleep(time.Duration(TIME_LATIDO) * time.Millisecond)
		}
	}
}

func (nr *NodoRaft) escuchaLatidos() error {
	lider := true

	for lider {
		timeE := rand.Intn(timeMaxLatido)
		select {
		case enviaLider := <-nr.Latidos:
			//	fmt.Println("He recibido latido")
			if enviaLider {
				//	nr.Hevotado = false
			}
		case <-time.After(time.Duration(3*TIME_LATIDO/2+timeE) * time.Millisecond):
			fmt.Println("Lider ha caido")
			lider = false

		}
	}
	return nil
}

//devuelve true si exito, false si fallo
func (nr *NodoRaft) nuevaEleccion() {
	fmt.Println("Nueva eleccion")
	nr.CurrentTerm++
	nr.Hevotado = true
	nr.VotedFor = nr.Yo //me voto a mí mismo
	args := ArgsPeticionVoto{nr.CurrentTerm, nr.Yo, nr.LastApplied, nr.CurrentTerm}
	var reply []RespuestaPeticionVoto

	votes := 1
	//enviar peticion de voto al resto de servidores
	for i := 0; i < len(nr.Nodos); i++ {
		reply = append(reply, RespuestaPeticionVoto{0, false})
		if i != nr.Yo {
			go nr.enviarPeticionVoto(i, &args, &reply[i])
			//	fmt.Println("Pido voto a ", i)
		}
	}
	fin := false
	fail := false
	for !fin {
		select {
		case _ = <-nr.Voto:
			votes++
			fmt.Println("Me han votado")
		case _ = <-nr.Latidos:
			fin = true
			fail = true
			fmt.Println("ha llegado un latido")
			// si llega latido qué pasa
		case <-time.After(time.Duration(TIME_VOTO) * time.Millisecond):
			fin = true
		}
	}
	//nr.Hevotado = false
	if !fail && votes > len(nr.Nodos)-votes { //eleccion ganada
		nr.IdLider = nr.Yo
	}
}

func (nr *NodoRaft) gestionRaft() {
	//esperar latidos
	fmt.Println("gestionando")
	nr.Hevotado = false
	for {
		if nr.IdLider == nr.Yo {
			nr.enviaLatidos()
		} else {
			nr.Hevotado = false
			nr.escuchaLatidos() //solo sale si no llega latido
			nr.nuevaEleccion()

		}
		//nr.Hevotado = false
	}
}
