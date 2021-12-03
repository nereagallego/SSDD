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

		i, _ := strconv.Atoi(args[1])
		var hosts []rpctimeout.HostPort
		//	hosts = append(hosts, rpctimeout.HostPort("localhost:3000"))
		//	hosts = append(hosts, rpctimeout.HostPort("localhost:3001"))
		//	hosts = append(hosts, rpctimeout.HostPort("localhost:3002"))

		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3000"))
		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3001"))
		hosts = append(hosts, rpctimeout.MakeHostPort("localhost", "3002"))
		// Parte Servidor

		arith := new()
		rpc.Register(arith)
		fmt.Println("Operacion registrada")

		_, e := net.Listen("tcp", string(hosts[i]))
		if e != nil {
			log.Fatal("listen error:", e)
		}

		//	conn, _ := l.Accept()
		fmt.Println("Accept correcto")
		//	if err != nil {
		//		continue
		//	}

		//	go rpc.ServeConn(conn)
		fmt.Println("Operacion rpc registrada")

		time.Sleep(100 * time.Millisecond)

		canal := make(chan raft.AplicaOperacion)
		fmt.Println("Intento crear un nodo")

		_ = raft.NuevoNodo(hosts, i, canal)
		for {
		}

	} else {
		fmt.Println("Usage: go run " + args[0] + " endpoint ")
	}

}
