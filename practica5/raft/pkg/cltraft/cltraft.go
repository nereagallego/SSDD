package main

import (
	"fmt"
	"log"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/maquinaestados"
	"raft/internal/raft"
	"time"
)

type Args struct {
	A, B int
}

func main() {
	args := os.Args
	if len(os.Args) == 5 {
		fmt.Println("conectado")
		var replay raft.ResultadoRemoto
		//	arg := Args{5, 7}
		//	var arg string = "holita"
		var master rpctimeout.HostPort
		master = rpctimeout.HostPort(args[1])
		var argumento maquinaestados.TipoOperacion
		argumento.Operacion = args[2]
		argumento.Clave = args[3]
		argumento.Valor = args[4]
		err := master.CallTimeout("NodoRaft.SometerOperacionRaft", &argumento, &replay, 2000*time.Millisecond)
		check.CheckError(err, "Main calltimeout")
		//out := client.Call("NodoRaft.SometerOperacionRaft", &arg, &replay)
		//	fmt.Println(out)

		if err != nil {
			log.Fatal("arith error:", err)
		}
		fmt.Println(replay.ValorADevolver)

		//	fmt.Println("Arith: %d*%d=%d", arg.A, arg.B, replay)
	} else {
		fmt.Println("Usage: go run " + args[0] + " endpointMaster operacion clave valor")
	}
}
