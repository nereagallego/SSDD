package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

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
	if len(os.Args) == 2 {

		var hosts []rpctimeout.HostPort
		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3000"))
		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3001"))
		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3002"))
		// Parte Servidor
		arith := new(Arith)
		rpc.Register(arith)
		i, _ := strconv.Atoi(args[1])
		l, e := net.Listen("tcp", hosts[i])
		if e != nil {
			log.Fatal("listen error:", e)
		}

		// Quitar el lanzamiento de la gorutina, pero no el código interno.
		// Solo se necesita para esta prueba dado que cliente y servidor estan,
		// aqui, juntos

		conn, _ := l.Accept()
		//	if err != nil {
		//		continue
		//	}

		go rpc.ServeConn(conn)

		time.Sleep(100 * time.Millisecond)

		canal := make(chan raft.AplicaOperacion)

		_ = raft.NuevoNodo(hosts, i, canal)

	} else {
		fmt.Println("Usage: go run " + args[0] + " endpoint ")
	}

}
