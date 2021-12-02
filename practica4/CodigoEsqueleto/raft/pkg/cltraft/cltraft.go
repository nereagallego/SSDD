package cltraft

import (
	"fmt"
	"log"
	"net/rpc"
	"practica4/CodigoEsqueleto/raft/comun/rpctimeout"
	"time"
)

type Args struct {
	A, B int
}

func main() {

	client, err := rpc.Dial("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var replay int
	args := Args{5, 7}
	err = rpctimeout.CallTimeout(client, &args, &replay,
		5*time.Millisecond)

	if err != nil {
		log.Fatal("arith error:", err)
	}

	fmt.Println("Arith: %d*%d=%d", args.A, args.B, replay)

}
