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
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"practica3/com"

	"golang.org/x/crypto/ssh"
)

type SshClient struct {
	Config *ssh.ClientConfig
	Server string
}

type maquina struct {
	ip     string
	puerto string
}

const (
	NORMAL   = iota // NORMAL == 0
	DELAY    = iota // DELAY == 1
	CRASH    = iota // CRASH == 2
	OMISSION = iota // IOTA == 3
)

type PrimesImpl struct {
	delayMaxMilisegundos int
	delayMinMiliSegundos int
	behaviourPeriod      int
	behaviour            int
	i                    int
	mutex                sync.Mutex
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

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func IsPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func FindPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if IsPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

func lanzaworker(maquina []maquina, id int) {
	ssh, err := NewSshClient(
		"a801950",
		maquina[id].ip,
		22,
		"/home/a801950/.ssh/id_rsa",
		"")
	fmt.Println("Lanzando worker... ")
	if err != nil {
		log.Printf("SSH init error %v", err)
	}
	for i := 30100; i < 50000; i++ {
		maquina[id].puerto = strconv.Itoa(i)

		output, _ := ssh.RunCommand("cd SSDD/practica1 && go run worker.go " + maquina[id].ip + ":" + maquina[id].puerto)
		fmt.Println(output)

	}
}

func handleClients(id int, maquina []maquina, clients chan net.Conn) {
	worker, err := rpc.DialHTTP("tcp", maquina[id].ip+":"+maquina[id].puerto)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply []int
	err = worker.Call("PrimesImpl.FindPrimes", interval, &reply)

	//	time.Sleep(time.Duration(60000) * time.Millisecond)
	tcpAddr, err := net.ResolveTCPAddr("tcp", maquina[id].ip+":"+maquina[id].puerto)
	checkError(err)

	worker, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	encoderS := gob.NewEncoder(worker)
	decoderS := gob.NewDecoder(worker)

	for {
		client, ok := <-clients
		if ok == false {
			break
		} else {
			encoder := gob.NewEncoder(client)
			decoder := gob.NewDecoder(client)
			var request com.Request

			err = decoder.Decode(&request)
			checkError(err)
			err = encoderS.Encode(&request)
			checkError(err)
			var reply com.Reply
			err = decoderS.Decode(&reply)
			checkError(err)
			err = encoder.Encode(&reply)
			checkError(err)

			client.Close()
		}
	}
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {
	p.mutex.Lock()
	if p.i%p.behaviourPeriod == 0 {
		p.behaviourPeriod = rand.Intn(20-2) + 2
		options := rand.Intn(100)
		if options > 90 {
			p.behaviour = CRASH
		} else if options > 60 {
			p.behaviour = DELAY
		} else if options > 40 {
			p.behaviour = OMISSION
		} else {
			p.behaviour = NORMAL
		}
		p.i = 0
	}
	p.i++
	p.mutex.Unlock()
	switch p.behaviour {
	case DELAY:
		seconds := rand.Intn(p.delayMaxMilisegundos-p.delayMinMiliSegundos) + p.delayMinMiliSegundos
		time.Sleep(time.Duration(seconds) * time.Millisecond)
		*primeList = findPrimes(interval)
	case CRASH:
		os.Exit(1)
	case OMISSION:
		option := rand.Intn(100)
		if option > 65 {
			time.Sleep(time.Duration(10000) * time.Second)
			*primeList = findPrimes(interval)
		} else {
			*primeList = findPrimes(interval)
		}
	case NORMAL:
		*primeList = findPrimes(interval)
	default:
		*primeList = findPrimes(interval)
	}
	return nil
}

func main() {
	args := os.Args
	fileMaquinas := args[1]
	file, err := os.Open(fileMaquinas)

	CONN_TYPE := "tcp"

	listener, err := net.Listen(CONN_TYPE, args[2])
	checkError(err)

	//handle errors while opening
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}
	var maquinas []maquina
	var m maquina
	nMaquinas := 0

	fileScanner := bufio.NewScanner(file)
	clients := make(chan net.Conn)

	for fileScanner.Scan() {
		ip := fileScanner.Text()
		m.ip = ip

		m.puerto = "30100"
		maquinas = append(maquinas, m)

		go lanzaworker(maquinas, nMaquinas)
		nMaquinas = nMaquinas + 1

	}
	time.Sleep(time.Duration(10000) * time.Millisecond)
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error while reading file: %s", err)
	}
	file.Close()
	fmt.Println("Estableciendo conexión con los workers...")
	for i := 0; i < nMaquinas; i++ {

		go handleClients(i, maquinas, clients)

	}
	primesImpl := new(PrimesImpl)
	primesImpl.delayMaxMilisegundos = 4000
	primesImpl.delayMinMiliSegundos = 2000
	primesImpl.behaviourPeriod = 4
	primesImpl.i = 1
	primesImpl.behaviour = NORMAL
	rpc.Register(primesImpl)
	//	rpc.HandleHTTP()

	//	http.Serve(listener, nil)

	fmt.Println("Esperando clientes...")
	for {
		conn, err := listener.Accept()
		clients <- conn
		checkError(err)
	}
}
