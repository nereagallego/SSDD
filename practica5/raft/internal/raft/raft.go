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

	CommitIndex int // Ultima entrada añadida al reistro
	LastApplied int // Ultima entrada añadida a la maquina de estados
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
	nr.LastApplied = -1
	nr.CommitIndex = -1
	nr.Latidos = make(chan bool)
	nr.Voto = make(chan bool)
	nr.Logs = []AplicaOperacion{}
	for i := 0; i < len(nodos); i++ {
		nr.NextIndex = append(nr.NextIndex, -1)
	}

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
	operacion TipoOperacion) (int, int, bool, int, string) {

	indice, mandato, EsLider, idLider, valorADevolver := -1, -1, nr.IdLider == nr.Yo, nr.IdLider, ""

	if !EsLider {
		return -1, -1, false, nr.IdLider, valorADevolver
	}
	// Vuestro codigo aqui
	prevLogIndex, prevLogTerm := -1, -1

	if len(nr.Logs) > 0 {
		prevLogIndex = nr.CommitIndex
		prevLogTerm = nr.Logs[nr.CommitIndex].Mandato
	}
	var entries []AplicaOperacion
	entries = append(entries, AplicaOperacion{nr.CurrentTerm, operacion})

	return nr.gestionaOperacion(entries, prevLogIndex, prevLogTerm, indice, mandato, EsLider, idLider) // !!!!!!
}

func (nr *NodoRaft) gestionaOperacion(entries []AplicaOperacion, prevLogIndex int, prevLogTerm int, indice int, mandato int, EsLider bool, idLider int) (int, int, bool, int, string) {

	for i := 0; i < len(entries); i++ {
		nr.Logs = append(nr.Logs, entries[i])
		nr.CommitIndex++
		fmt.Println("Entrada añadida")
	}

	out, mayoria := nr.sendMsg(entries, prevLogIndex, prevLogTerm)
	valorADevolver := ""

	if out {
		indice = prevLogIndex + 1
		mandato = nr.CurrentTerm
		EsLider = nr.Yo == nr.IdLider
		idLider = nr.IdLider
		if mayoria {
			// aplicar a la maquina de estados
		}
	}
	return indice, mandato, EsLider, idLider, valorADevolver
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
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {

	reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider,
		reply.ValorADevolver = nr.someterOperacion(operacion)
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
	Term         int // Mandato de quien solicita voto
	CandidateId  int // ID candidato que solicita voto
	LastLogIndex int // Indice de la última entrada en el registro
	LastLogTerm  int // Mandato de la última entrada en el registro
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
	Term        int  // Mandato actual de a quien solicitas voto
	VoteGranted bool // True si y solo si me votan
}

//
// PedirVoto
// ===========
//
// Metodo para RPC PedirVoto (me piden que les vote)
//
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	reply.Term = nr.CurrentTerm
	ultimoMandato := -1
	if nr.CommitIndex != -1 {
		ultimoMandato = nr.Logs[nr.CommitIndex].Mandato
	}
	reply.VoteGranted = (args.LastLogIndex > ultimoMandato) || (args.LastLogTerm == ultimoMandato && args.LastLogIndex >= nr.CommitIndex)
	if nr.Hevotado && !reply.VoteGranted {
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
	if args.Term < nr.CurrentTerm {
		reply.Success = false
	} else if len(args.Entries) == 0 {
		if nr.CurrentTerm < args.Term {
			nr.CurrentTerm = args.Term
		}
		nr.IdLider = args.LeaderId
		nr.Latidos <- true
		reply.Success = true
	} else if (args.PrevLogIndex != -1 && (nr.CommitIndex == -1 || nr.Logs[args.PrevLogIndex].Mandato != args.PrevLogTerm)) || (args.PrevLogIndex == -1 && len(nr.Logs) != 0) {
		reply.Success = false
	} else {
		nr.CommitIndex = args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+i < nr.CommitIndex {
				nr.Logs[args.PrevLogIndex+i] = args.Entries[i]
			} else {
				nr.Logs = append(nr.Logs, args.Entries[i])
				//?
			}
			nr.CommitIndex++
		}
		reply.Success = true
		fmt.Println("entrada añadida")
	}
	reply.Term = nr.CurrentTerm
	return nil
}

func (nr *NodoRaft) countSuccessReply(reply []AppendEntriesReply) int {
	count := 0
	for i := 0; i < len(nr.Nodos); i++ {
		if reply[i].Success {
			count++
		} else if reply[i].Term > nr.CurrentTerm {
			return -1
		} else {

		}
	}
	return count
}

func (nr *NodoRaft) sendMsg(entradas []AplicaOperacion, prevLogIndex int, prevLogTerm int) (bool, bool) {

	arg := AppendEntriesArgs{nr.CurrentTerm, nr.IdLider, prevLogIndex, prevLogTerm, entradas, nr.CommitIndex}

	var reply []AppendEntriesReply

	for i := 0; i < len(nr.Nodos); i++ {
		aux := AppendEntriesReply{0, false}
		reply = append(reply, aux)
		nr.NextIndex[i] = nr.CommitIndex
	}

	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			go nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 100*time.Millisecond)
		}
	}

	time.Sleep(time.Duration(TIME_MSG) * time.Millisecond)
	count := nr.countSuccessReply(reply)
	if count == -1 {
		return false, false
	}
	return true, count > (len(nr.Nodos) - count)
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
		out, _ := nr.sendMsg(e, 0, 0)
		if !out {
			soyLider = false
		}
		select {
		case _ = <-nr.Latidos:
			soyLider = false
		case <-time.After(time.Duration(TIME_LATIDO) * time.Millisecond):

		}

	}
}

func (nr *NodoRaft) escuchaLatidos() error {
	lider := true

	for lider {
		timeE := rand.Intn(timeMaxLatido)
		select {
		case _ = <-nr.Latidos:

		case <-time.After(time.Duration(3*TIME_LATIDO/2+timeE) * time.Millisecond):
			fmt.Println("Lider ha caido")
			lider = false
		}
	}
	return nil
}

func (nr *NodoRaft) receive() (bool, int) {
	fin := false
	votes := 1
	fail := false
	for !fin {
		select {
		case _ = <-nr.Voto:
			votes++
		case _ = <-nr.Latidos: // ha llegado un latido
			fin = true
			fail = true
		case <-time.After(time.Duration(TIME_VOTO) * time.Millisecond):
			fin = true
		}
	}
	return fail, votes
}

//devuelve true si exito, false si fallo
func (nr *NodoRaft) nuevaEleccion() {
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
		}
	}
	fail, votes := nr.receive()
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
