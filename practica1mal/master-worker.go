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
	"net"
	"os"
	"practica1/com"
	"time"

	"golang.org/x/crypto/ssh"
)

type SshClient struct {
	Config *ssh.ClientConfig
	Server string
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

// sendRequest envía una petición (id, interval) al servidor. Una petición es un par id
// (el identificador único de la petición) e interval, el intervalo en el cual se desea que el servidor encuentre los
// números primos. La petición se serializa utilizando el encoder y una vez enviada la petición
// se almacena en una estructura de datos, junto con una estampilla
// temporal. Para evitar condiciones de carrera, la estructura de datos compartida se almacena en una Goroutine
// (handleRequests) y que controla los accesos a través de canales síncronos. En este caso, se añade una nueva
// petición a la estructura de datos mediante el canal addChan
func sendReply(ch chan com.Reply, encoder *gob.Encoder) {

	//	for reply, ok := <-ch; ok == true; {
	//for cap(ch) > 0 {
	for {
		reply, ok := <-ch
		if ok == false {
			break
		} else {
			err := encoder.Encode(reply)
			checkError(err)
		}
	}
}

func receiveReply(decoder *gob.Decoder, repl chan com.Reply) {
	for {
		var replay com.Reply
		err := decoder.Decode(&replay)
		checkError(err)
		repl <- replay
	}
}

func lanzaWorker(ip string, puerto string, req chan com.Request, repl chan com.Reply, permiso chan int) {
	ssh, err := NewSshClient(
		"a801950",
		ip,
		22,
		"/home/a801950/.ssh/id_rsa",
		"")
	fmt.Println("Lanzando servidor...")
	permiso <- 1
	if err != nil {
		log.Printf("SSH init error %v", err)
	} else {
		output, err := ssh.RunCommand("cd SSDD/practica1 && go run worker.go " + ip + " " + puerto)
		fmt.Println(output)
		if err != nil {
			log.Printf("SSH run command error %v", err)
		}
	}

}

func conectaWorker(maquina string, req chan com.Request, repl chan com.Reply, permiso chan int) {
	_ = <-permiso
	time.Sleep(time.Duration(8000) * time.Millisecond)
	endpoint := maquina
	tcpAddr, err := net.ResolveTCPAddr("tcp", endpoint)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	fmt.Println("Worker conectado")

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	go receiveReply(decoder, repl)
	for {
		request, ok := <-req
		if ok == false {
			break
		} else {
			err = encoder.Encode(request)
			checkError(err)
		}
	}
}

func main() {

	ch := make(chan com.Request)
	ch2 := make(chan com.Reply)
	ch1 := make(chan int)

	args := os.Args

	// El primer argumento, elemento cero de la matriz,
	// es el nombre del binario llamado.
	fileMaquinas := args[1]

	file, err := os.Open(fileMaquinas)

	//handle errors while opening
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}

	fileScanner := bufio.NewScanner(file)

	// read line by line
	for fileScanner.Scan() {
		ip := fileScanner.Text()
		fileScanner.Scan()
		go lanzaWorker(ip, fileScanner.Text(), ch, ch2, ch1)
		go conectaWorker(ip+":"+fileScanner.Text(), ch, ch2, ch1)
	}
	// handle first encountered error while reading
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error while reading file: %s", err)
	}

	file.Close()
	time.Sleep(time.Duration(8000) * time.Millisecond)
	CONN_TYPE := "tcp"
	CONN_HOST := "155.210.154.205"
	CONN_PORT := "30010"

	fmt.Println("Esperando clientes")
	listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	checkError(err)

	conn, err := listener.Accept()
	defer conn.Close()
	checkError(err)

	// TO DO
	fmt.Println("Cliente conectado")

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	go sendReply(ch2, encoder)

	var request com.Request
	err = decoder.Decode(&request)

	for err == nil {

		checkError(err)
		ch <- request
		err = decoder.Decode(&request)
	}

}
