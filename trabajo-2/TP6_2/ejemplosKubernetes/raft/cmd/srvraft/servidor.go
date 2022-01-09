package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"raft/internal/comun/rpctimeout"
	"raft/internal/maquinaestados"

	"raft/internal/raft"
)

type Args struct {
	A, B int
}

type Arith int

func (t *Arith) Mul(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func main() {
	args := os.Args
	if len(os.Args) >= 3 {

		//	me, _ := strconv.Atoi(os.Args[1])
		miIp := os.Args[1]
		numReplicas, _ := strconv.Atoi(os.Args[2])
		dom := os.Args[3]

		//check.CheckError(err, "Main, mal numero entero de indice de nodo:")

		var nodos []rpctimeout.HostPort
		// Resto de argumento son los end points como strings
		// De todas la replicas-> pasarlos a HostPort
		//	for _, endPoint := range os.Args[3:] {
		//		nodos = append(nodos, rpctimeout.HostPort(endPoint))
		//	}
		me := -1
		for i := 0; i < numReplicas; i++ {
			ip := "r-" + strconv.Itoa(i) + "." + dom
			nodos = append(nodos, rpctimeout.HostPort(ip))
			if ip == miIp {
				me = i
			}
		}
		if me == -1 {
			os.Exit(1)
		}

		// Parte Servidor
		nr := raft.NuevoNodo(nodos, me, make(chan maquinaestados.AplicaOperacion, 1000))
		rpc.Register(nr)
		//	for {
		//	}

		//fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

		l, err := net.Listen("tcp", os.Args[2:][me])
		log.Println(err)
		//check.CheckError(err, "Main listen error:")

		rpc.Accept(l)

	} else {
		log.Println("Usage: go run " + args[0] + " endpoint ")
	}

}
