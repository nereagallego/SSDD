package main

import (
	"fmt"
	"log"
	"os"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"time"
)

type Args struct {
	A, B int
}

func main() {
	args := os.Args
	if len(os.Args) == 2 {
		//	client, err := rpc.Dial("tcp", args[1])
		//	if err != nil {
		//		log.Fatal("dialing:", err)
		//	}
		fmt.Println("conectado")
		var replay raft.ResultadoRemoto
		//	arg := Args{5, 7}
		//	var arg string = "holita"
		var master rpctimeout.HostPort
		master = rpctimeout.HostPort(args[1])
		var argumento raft.TipoOperacion
		argumento.Operacion = "leer"
		argumento.Clave = "holita"
		err := master.CallTimeout("NodoRaft.SometerOperacionRaft", &argumento, &replay, 2000*time.Millisecond)
		//out := client.Call("NodoRaft.SometerOperacionRaft", &arg, &replay)
		//	fmt.Println(out)

		if err != nil {
			log.Fatal("arith error:", err)
		}
		fmt.Println(replay.ValorADevolver)

		//	fmt.Println("Arith: %d*%d=%d", arg.A, arg.B, replay)
	} else {
		fmt.Println("Usage: go run " + args[0] + " endpointMaster")
	}
}
