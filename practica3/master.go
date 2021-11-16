/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 1
 */
package main

import (
	"bufio"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"practica3/com"
	"practica3/maton"

	"golang.org/x/crypto/ssh"
)

type SshClient struct {
	Config *ssh.ClientConfig
	Server string
}

// type maquina struct {
// 	ip     string
// 	puerto string
// }

const (
	NORMAL   = iota // NORMAL == 0
	DELAY    = iota // DELAY == 1
	CRASH    = iota // CRASH == 2
	OMISSION = iota // IOTA == 3
)

type peticion struct {
	interval  com.TPInterval
	respuesta chan ([]int)
}

type PrimesImpl struct {
	req                  chan (peticion)
	i                    int
	mutex                sync.Mutex
	delayMaxMilisegundos int
	delayMinMiliSegundos int
	behaviourPeriod      int
	behaviour            int
	maton                *maton.Maton
}

type Maquina struct {
	Ip     string
	Puerto string
}

func NewSshClient(user string, host string, port int, privateKeyPath string, privateKeyPassword string) (*SshClient, error) {
	// read private key file
	pemBytes, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("Reading private key file failed %v", err)
	}
	// create signer
	signer, err := signerFromPem(pemBytes, []byte(privateKeyPassword))
	if err != nil {
		return nil, err
	}
	// build SSH client config
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			// use OpenSSH's known_hosts file if you care about host validation
			return nil
		},
	}

	client := &SshClient{
		Config: config,
		Server: fmt.Sprintf("%v:%v", host, port),
	}

	return client, nil
}

// Opens a new SSH connection and runs the specified command
// Returns the combined output of stdout and stderr
func (s *SshClient) RunCommand(cmd string) (string, error) {
	// open connection
	conn, err := ssh.Dial("tcp", s.Server, s.Config)
	if err != nil {
		return "", fmt.Errorf("Dial to %v failed %v", s.Server, err)
	}
	defer conn.Close()

	// open session
	session, err := conn.NewSession()
	if err != nil {
		return "", fmt.Errorf("Create session for %v failed %v", s.Server, err)
	}
	defer session.Close()

	// run command and capture stdout/stderr
	output, err := session.CombinedOutput(cmd)

	return fmt.Sprintf("%s", output), err
}

func signerFromPem(pemBytes []byte, password []byte) (ssh.Signer, error) {

	// read pem block
	err := errors.New("Pem decode failed, no key found")
	pemBlock, _ := pem.Decode(pemBytes)
	if pemBlock == nil {
		return nil, err
	}

	// handle encrypted key
	if x509.IsEncryptedPEMBlock(pemBlock) {
		// decrypt PEM
		pemBlock.Bytes, err = x509.DecryptPEMBlock(pemBlock, []byte(password))
		if err != nil {
			return nil, fmt.Errorf("Decrypting PEM block failed %v", err)
		}

		// get RSA, EC or DSA key
		key, err := parsePemBlock(pemBlock)
		if err != nil {
			return nil, err
		}

		// generate signer instance from key
		signer, err := ssh.NewSignerFromKey(key)
		if err != nil {
			return nil, fmt.Errorf("Creating signer from encrypted key failed %v", err)
		}

		return signer, nil
	} else {
		// generate signer instance from plain key
		signer, err := ssh.ParsePrivateKey(pemBytes)
		if err != nil {
			return nil, fmt.Errorf("Parsing plain private key failed %v", err)
		}

		return signer, nil
	}
}

func parsePemBlock(block *pem.Block) (interface{}, error) {
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("Parsing PKCS private key failed %v", err)
		} else {
			return key, nil
		}
	case "EC PRIVATE KEY":
		key, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("Parsing EC private key failed %v", err)
		} else {
			return key, nil
		}
	case "DSA PRIVATE KEY":
		key, err := ssh.ParseDSAPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("Parsing DSA private key failed %v", err)
		} else {
			return key, nil
		}
	default:
		return nil, fmt.Errorf("Parsing private key failed, unsupported key type %q", block.Type)
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func isPrime(n int) (foundDivisor bool) {
	foundDivisor = false

	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func findPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if isPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

func lanzaworker(maquina []Maquina, id int, puertoInicio int) {
	ssh, err := NewSshClient(
		"a801950",
		maquina[id].Ip,
		22,
		"/home/a801950/.ssh/id_rsa",
		"")
	fmt.Println("Lanzando worker... ")
	//	sId := strconv.Itoa(id)
	//	sCor := strconv.Itoa(coordinador)
	if err != nil {
		log.Printf("SSH init error %v", err)
	}
	for i := puertoInicio; i < 50000; i++ {
		maquina[id].Puerto = strconv.Itoa(i)

		output, _ := ssh.RunCommand("cd SSDD/practica3 && go run worker.go " + maquina[id].Ip + ":" + maquina[id].Puerto)
		fmt.Println(output)

	}
}

func lanzaReplicaMaster(maquina []Maquina, id int, puertoInicio int, fileMaquinas string, ficheroMaton string) {
	ssh, err := NewSshClient(
		"a801950",
		maquina[id].Ip,
		22,
		"/home/a801950/.ssh/id_rsa",
		"")
	fmt.Println("Lanzando replicas del Master... ")
	sId := strconv.Itoa(id)
	//	sCor := strconv.Itoa(coordinador)
	if err != nil {
		log.Printf("SSH init error %v", err)
	}
	for i := puertoInicio; i < 50000; i++ {
		maquina[id].Puerto = strconv.Itoa(i)

		output, _ := ssh.RunCommand("cd SSDD/practica3 && go run master.go " + fileMaquinas + " " + maquina[id].Ip + ":" + maquina[id].Puerto + " 0 " + ficheroMaton + " no " + sId)
		fmt.Println(output)

	}
}

func handleClients(id int, maquina []Maquina, peticiones chan peticion) {
	worker, err := rpc.DialHTTP("tcp", maquina[id].Ip+":"+maquina[id].Puerto)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply []int
	fmt.Println("Lanzado woker %d", id)
	for {
		pet, ok := <-peticiones
		if ok == false {
			fmt.Println("peticion MAL. ERROR!")
			break
		} else {
			done := worker.Go("PrimesImpl.FindPrimes", pet.interval, &reply, make(chan *rpc.Call, 1)).Done
			select {
			case _ = <-done:
				pet.respuesta <- reply
			case <-time.After(1500 * time.Millisecond):
				fmt.Errorf("Timeout in CallTimout\n")
				peticiones <- pet
			}
		}
	}
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	p.mutex.Lock()
	var pet peticion
	pet.interval = interval
	pet.respuesta = make(chan []int)
	p.req <- pet
	p.mutex.Unlock()
	*primeList = <-pet.respuesta
	return nil

}

func maquinasStructure(maquinas []Maquina, fileMaquinas string, puertoIni int) int {
	file, err := os.Open(fileMaquinas)
	checkError(err)
	nMaquinas := 0
	var m Maquina
	fileScanner := bufio.NewScanner(file)
	p := strconv.Itoa(puertoIni)

	for fileScanner.Scan() {
		ip := fileScanner.Text()
		m.Ip = ip

		m.Puerto = p
		maquinas = append(maquinas, m)

		//	go lanzaworker(maquinas, nMaquinas, puertoInicio)

		nMaquinas = nMaquinas + 1

	}
	file.Close()

	return nMaquinas
}

func lineasFichero(fileMaquinas string) int {
	file, err := os.Open(fileMaquinas)
	checkError(err)
	nMaquinas := 0
	//	var m Maquina
	fileScanner := bufio.NewScanner(file)
	//	p := strconv.Itoa(puertoIni)

	for fileScanner.Scan() {
		_ = fileScanner.Text()
		//		m.Ip = ip

		//	m.Puerto = p
		//	maquinas = append(maquinas, m)

		//	go lanzaworker(maquinas, nMaquinas, puertoInicio)

		nMaquinas = nMaquinas + 1

	}
	file.Close()

	return nMaquinas
}

func gestionWorkers(maquinas []Maquina, pIni int, peticiones chan (peticion), fileMaquinas string) int {
	file, err := os.Open(fileMaquinas)
	checkError(err)
	nMaquinas := 0
	var m Maquina
	fileScanner := bufio.NewScanner(file)
	p := strconv.Itoa(pIni)

	for fileScanner.Scan() {
		ip := fileScanner.Text()
		m.Ip = ip

		m.Puerto = p
		maquinas = append(maquinas, m)

		//	go lanzaworker(maquinas, nMaquinas, puertoInicio)

		nMaquinas = nMaquinas + 1

	}
	file.Close()
	for i := 0; i < nMaquinas; i++ {
		go lanzaworker(maquinas, i, pIni)
	}

	time.Sleep(time.Duration(20000*nMaquinas) * time.Millisecond)

	fmt.Println("Estableciendo conexión con los workers...")
	for i := 0; i < nMaquinas; i++ {

		go handleClients(i, maquinas, peticiones)

	}
	return nMaquinas
}

func main() {
	if len(os.Args) == 8 {
		args := os.Args
		soyLider := (args[5] == "Coordinador")
		fmt.Println(soyLider)
		// if soyLider {
		// 	id, _ := strconv.Atoi(args[6])
		// 	fileMaquinas := args[1]
		// 	nM, _ := strconv.Atoi(args[7])
		// 	ficheroMaton := args[4]

		// 	CONN_TYPE := "tcp"

		// 	listener, err := net.Listen(CONN_TYPE, args[2])
		// 	puertoInicio, _ := strconv.Atoi(args[3])
		// 	checkError(err)

		// 	//handle errors while opening

		// 	var maquinas []Maquina

		// 	nMaquinas := maquinasStructure(maquinas, fileMaquinas, puertoInicio)
		// 	peticiones := make(chan peticion)

		// 	gestionWorkers(maquinas, nMaquinas, puertoInicio, peticiones)

		// 	soyLider := make(chan string)

		// 	primesImpl := new(PrimesImpl)
		// 	primesImpl.req = peticiones
		// 	primesImpl.i = nMaquinas
		// 	algM := maton.New(id, nM, ficheroMaton, &soyLider)
		// 	primesImpl.maton = algM

		// 	rpc.Register(primesImpl)
		// 	rpc.HandleHTTP()

		// 	fmt.Println("Esperando clientes...")
		// 	http.Serve(listener, nil)
		//	} else {
		id, _ := strconv.Atoi(args[6])
		nM, _ := strconv.Atoi(args[7])
		fileMaquinas := args[1]
		ficheroMaton := args[4]
		CONN_TYPE := "tcp"

		listener, err := net.Listen(CONN_TYPE, args[2])
		puertoInicio, _ := strconv.Atoi(args[3])
		checkError(err)

		//	Lider := make(chan string)
		algM := maton.New(id, nM, ficheroMaton) //, &Lider)
		fmt.Println("creo matón")
		// if soyLider {
		// 	fmt.Println("lider inicial")
		// 	Lider <- "ok"
		// 	fmt.Println("lider inicial")
		//	}
		for {
			//	_ = <-Lider
			if algM.Coordinador == id {
				fmt.Println("Soy el coordinador")
				var maquinas []Maquina

				//	nMaquinas := maquinasStructure(maquinas, fileMaquinas, puertoInicio)
				peticiones := make(chan peticion)

				nMaquinas := gestionWorkers(maquinas, puertoInicio, peticiones, fileMaquinas)

				primesImpl := new(PrimesImpl)
				primesImpl.req = peticiones
				primesImpl.i = nMaquinas
				primesImpl.maton = algM

				rpc.Register(primesImpl)
				rpc.HandleHTTP()

				fmt.Println("Esperando clientes...")
				http.Serve(listener, nil)
			} else {
				time.Sleep(20 * time.Second)
			}

		}
		//	}

	} else {
		fmt.Println("Usage: go run master.go fileMaquinas.txt <ip:port> puertoInicioWorkers ficheroMaton.txt funcionaComoMaster id nMasters")
	}
}
