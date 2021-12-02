package main

import (
	"log"
	"net"
	"net/rpc"
	"time"
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

	// Parte Servidor
	arith := new(Arith)
	rpc.Register(arith)

	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// Quitar el lanzamiento de la gorutina, pero no el código interno.
	// Solo se necesita para esta prueba dado que cliente y servidor estan,
	// aqui, juntos
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			go rpc.ServeConn(conn)
		}
	}()

	time.Sleep(100 * time.Millisecond)

}
