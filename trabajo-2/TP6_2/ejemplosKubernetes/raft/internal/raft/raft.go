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
	"raft/internal/maquinaestados"
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
	Logs        []maquinaestados.AplicaOperacion

	CommitIndex int // Ultima entrada añadida al reistro
	LastApplied int // Ultima entrada añadida a la maquina de estados
	NextIndex   []int
	MatchIndex  []int
	Latidos     chan bool

	Hevotado       bool
	Voto           chan bool
	CanalAplicar   chan maquinaestados.AplicaOperacion
	CanalRespuesta chan string
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
	canalAplicar chan maquinaestados.AplicaOperacion) *NodoRaft {
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
	nr.Logs = []maquinaestados.AplicaOperacion{}
	nr.CanalRespuesta = make(chan string)
	maquinaestados.NuevaMaquinaEstados(canalAplicar, nr.CanalRespuesta)
	nr.CanalAplicar = canalAplicar
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
	log.Println("Nodo creado")
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
	operacion maquinaestados.TipoOperacion) (string, int, int, bool, int) {

	indice, mandato, EsLider, idLider := -1, nr.CurrentTerm, nr.IdLider == nr.Yo, nr.IdLider

	if !EsLider {
		return "error", -1, nr.CurrentTerm, false, nr.IdLider
	}
	// Vuestro codigo aqui
	prevLogIndex, prevLogTerm := -1, -1

	if len(nr.Logs) > 0 {
		prevLogIndex = nr.CommitIndex
		prevLogTerm = nr.Logs[nr.CommitIndex].Mandato
	}
	var entries []maquinaestados.AplicaOperacion
	entries = append(entries, maquinaestados.AplicaOperacion{Mandato: nr.CurrentTerm, Operacion: operacion})

	return nr.gestionaOperacion(entries, prevLogIndex, prevLogTerm, indice, mandato, EsLider, idLider) // !!!!!!
}

func (nr *NodoRaft) gestionaOperacion(entries []maquinaestados.AplicaOperacion, prevLogIndex int, prevLogTerm int, indice int, mandato int, EsLider bool, idLider int) (string, int, int, bool, int) {

	nr.Mux.Lock()
	for i := 0; i < len(entries); i++ {
		nr.Logs = append(nr.Logs, entries[i])
		nr.CommitIndex++
		log.Println("Entrada añadida")
	}
	nr.Mux.Unlock()

	out, mayoria, alive := nr.sendMsg(entries, prevLogIndex, prevLogTerm)
	valorADevolver := "error"

	if out {
		nr.Mux.Lock()
		indice = prevLogIndex + 1
		mandato = nr.CurrentTerm
		EsLider = nr.Yo == nr.IdLider
		idLider = nr.IdLider
		nr.Mux.Unlock()
		if mayoria {
			valorADevolver = nr.hayMayoria(alive, valorADevolver)
		}
	}
	return valorADevolver, indice, mandato, EsLider, idLider
}

func (nr *NodoRaft) hayMayoria(alive []bool, valorADevolver string) string {
	nr.Mux.Lock()
	for i := nr.LastApplied + 1; i <= nr.CommitIndex; i++ {
		nr.CanalAplicar <- nr.Logs[i]
		valorADevolver = <-nr.CanalRespuesta
		nr.LastApplied++
	}
	nr.Mux.Unlock()
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo && alive[i] {
			go nr.Nodos[i].CallTimeout("NodoRaft.AplicarOperacion", nr.LastApplied, Vacio{}, time.Duration(TIME_MSG)*time.Millisecond)
		}
	}
	log.Println("Aplicada a la maquina de estados")
	return valorADevolver
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

type EstadoRegistro struct {
	LastApplied int
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

func (nr *NodoRaft) AplicarOperacion(lastApplied int, reply *Vacio) error {
	log.Println("aplico a la maquina de estados")
	for i := nr.LastApplied + 1; i <= lastApplied; i++ {
		nr.CanalAplicar <- nr.Logs[i]
		_ = <-nr.CanalRespuesta
		nr.LastApplied++
	}
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion *maquinaestados.TipoOperacion, reply *ResultadoRemoto) error {
	log.Println("Someter operacion ", operacion.Operacion, " ", operacion.Clave, " ", operacion.Valor)

	reply.ValorADevolver, reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider = nr.someterOperacion(*operacion)
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
		nr.Mux.Lock()
		ultimoMandato = nr.Logs[nr.CommitIndex].Mandato
		nr.Mux.Unlock()
	}
	nr.Mux.Lock()
	reply.VoteGranted = (args.LastLogTerm > ultimoMandato) || (args.LastLogTerm == ultimoMandato && args.LastLogIndex >= nr.CommitIndex && args.Term >= nr.CurrentTerm)
	if nr.Hevotado || !reply.VoteGranted {

		reply.VoteGranted = false

	} else {
		log.Println("He votado a ", args.CandidateId)
		nr.Hevotado = true
		nr.VotedFor = args.CandidateId
	}
	nr.Mux.Unlock()

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
		log.Println("me han votado")
		nr.Voto <- true
	}
	return true
}

type Entrada interface{}

type AppendEntriesArgs struct {
	Term         int                              // leader's term
	LeaderId     int                              // leader ID
	PrevLogIndex int                              // index of log entry immediaty preceding new ones
	PrevLogTerm  int                              // term of prevLogIndex entry
	Entries      []maquinaestados.AplicaOperacion // log entries to store (enpty for heartbeat)
	LeaderCommit int                              // leader commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Alive   bool // para reenviar registros anteriores a seguidores que continuan vivos
}

func (nr *NodoRaft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	reply.Alive = true
	if args.Term < nr.CurrentTerm {
		reply.Success = false
	} else if len(args.Entries) == 0 {
		log.Println("latido de: ", args.LeaderId)
		if nr.CurrentTerm < args.Term {
			nr.CurrentTerm = args.Term
		}
		nr.IdLider = args.LeaderId
		nr.Latidos <- true
		reply.Success = true
	} else if (args.PrevLogIndex != -1 && (nr.CommitIndex == -1 || nr.Logs[args.PrevLogIndex].Mandato != args.PrevLogTerm)) || (args.PrevLogIndex == -1 && len(nr.Logs) != 0) {
		reply.Success = false
	} else {
		nr.Mux.Lock()
		nr.CommitIndex = args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+i < nr.CommitIndex {
				nr.Logs[args.PrevLogIndex+i] = args.Entries[i]
			} else {
				nr.Logs = append(nr.Logs, args.Entries[i])
			}
			nr.CommitIndex++
			log.Println("entrada añadida")
		}
		reply.Success = true
		nr.Mux.Unlock()

	}
	reply.Term = nr.CurrentTerm
	return nil
}

func (nr *NodoRaft) entradas(index int) []maquinaestados.AplicaOperacion {
	var entradas []maquinaestados.AplicaOperacion
	for i := index; i <= nr.CommitIndex; i++ {
		entradas = append(entradas, nr.Logs[i])
	}
	return entradas
}

func (nr *NodoRaft) countSuccessReply(reply []AppendEntriesReply, alive []bool) (int, []bool) {
	count := 0
	//	fin := false
	noAlivenoSucced := true

	for noAlivenoSucced {
		count = 1
		noAlivenoSucced = false
		for i := 0; i < len(nr.Nodos); i++ {
			if i != nr.Yo {
				if reply[i].Success {
					count++
					alive[i] = true
				} else if reply[i].Term > nr.CurrentTerm {
					return -1, alive
				} else if reply[i].Alive {
					noAlivenoSucced = nr.sigueVivo(noAlivenoSucced, i, reply)
				}
			}
		}
		time.Sleep(time.Duration(TIME_MSG) * 5 * time.Millisecond)

	}
	return count, alive
}

func (nr *NodoRaft) sigueVivo(noAlivenoSucced bool, i int, reply []AppendEntriesReply) bool {
	nr.NextIndex[i]--
	noAlivenoSucced = true
	ultimoMandato := -1
	if nr.NextIndex[i]-1 >= 0 {
		ultimoMandato = nr.Logs[nr.NextIndex[i]-1].Mandato
	}
	arg := AppendEntriesArgs{nr.CurrentTerm, nr.IdLider, nr.NextIndex[i] - 1, ultimoMandato, nr.entradas(nr.NextIndex[i]), nr.CommitIndex}
	go nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 100*time.Millisecond)
	log.Println("Mando al nodo ", i, " la entrada ", nr.NextIndex[i])
	return noAlivenoSucced
}

func (nr *NodoRaft) sendMsg(entradas []maquinaestados.AplicaOperacion, prevLogIndex int, prevLogTerm int) (bool, bool, []bool) {
	arg := AppendEntriesArgs{nr.CurrentTerm, nr.IdLider, prevLogIndex, prevLogTerm, entradas, nr.CommitIndex}

	var reply []AppendEntriesReply
	var alive []bool

	for i := 0; i < len(nr.Nodos); i++ {
		aux := AppendEntriesReply{0, false, false}
		reply = append(reply, aux)
		if len(entradas) > 0 {
			nr.NextIndex[i] = nr.CommitIndex
			alive = append(alive, false)
		}
	}

	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			go nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", &arg, &reply[i], 100*time.Millisecond)
		}
	}

	time.Sleep(time.Duration(TIME_MSG) * time.Millisecond)
	count := 0
	if len(entradas) > 0 {
		log.Println("busco mayoria")
		count, alive = nr.countSuccessReply(reply, alive)
	}
	if count == -1 {
		return false, false, alive
	}
	return true, count > (len(nr.Nodos) - count), alive
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (nr *NodoRaft) enviaLatidos() {
	soyLider := true
	log.Println("soy lider en el mandato ", nr.CurrentTerm)
	var e []maquinaestados.AplicaOperacion
	for soyLider {
		out, _, _ := nr.sendMsg(e, 0, 0)
		if !out {
			soyLider = false
		}
		select {
		case _ = <-nr.Latidos:
			log.Println("era lider y ha llegado un latido")
			soyLider = false
		case <-time.After(time.Duration(TIME_LATIDO) * time.Millisecond):

		}

	}
	log.Println("ya no soy lider")
}

func (nr *NodoRaft) escuchaLatidos() error {
	lider := true
	log.Println("escucho latidos")
	for lider {
		timeE := rand.Intn(timeMaxLatido)
		select {
		case _ = <-nr.Latidos:
		case <-time.After(time.Duration(3*(TIME_LATIDO)/2+timeE) * time.Millisecond):
			nr.Logger.Println("ha caido el lider")
			log.Println("Lider ha caido")
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
			log.Println("me han votado")
			votes++
		case _ = <-nr.Latidos: // ha llegado un latido
			log.Println("Esperaba voto y ha llegado latido")
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
	log.Println("inicio eleccion")
	nr.CurrentTerm++
	nr.Hevotado = true
	nr.VotedFor = nr.Yo //me voto a mí mismo
	ultimoMandato := -1
	if len(nr.Logs) > 0 {
		ultimoMandato = nr.Logs[nr.CommitIndex].Mandato
	}
	args := ArgsPeticionVoto{nr.CurrentTerm, nr.Yo, nr.CommitIndex, ultimoMandato}
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
	log.Println("gestionando")
	nr.Hevotado = false
	time.Sleep(time.Duration(7000) * time.Millisecond)
	for {
		if nr.IdLider == nr.Yo {
			nr.Hevotado = false
			nr.enviaLatidos()
		} else {
			nr.Hevotado = false
			nr.escuchaLatidos() //solo sale si no llega latido
			nr.nuevaEleccion()

		}
	}
}
