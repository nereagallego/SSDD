package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"raft/internal/comun/rpctimeout"

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
	if len(os.Args) >= 2 {

		me, _ := strconv.Atoi(os.Args[1])
		//check.CheckError(err, "Main, mal numero entero de indice de nodo:")

		var nodos []rpctimeout.HostPort
		// Resto de argumento son los end points como strings
		// De todas la replicas-> pasarlos a HostPort
		for _, endPoint := range os.Args[2:] {
			nodos = append(nodos, rpctimeout.HostPort(endPoint))
		}

		// Parte Servidor
		nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
		rpc.Register(nr)
		//	for {
		//	}

		//fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

		l, _ := net.Listen("tcp", os.Args[2:][me])
		//	check.CheckError(err, "Main listen error:")

		rpc.Accept(l)

	} else {
		fmt.Println("Usage: go run " + args[0] + " endpoint ")
	}

}
