package main

import (
	"fmt"
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
		var replay raft.ResultadoRemoto
		//	arg := Args{5, 7}
		//	var arg string = "holita"
		var master rpctimeout.HostPort
		master = rpctimeout.HostPort(args[1])
		var argumento maquinaestados.TipoOperacion
		argumento.Operacion = args[2]
		argumento.Clave = args[3]
		argumento.Valor = args[4]
		var reply raft.EstadoRemoto
		err := master.CallTimeout("NodoRaft.ObtenerEstadoNodo", &raft.Vacio{}, &reply, 10*time.Millisecond)
		//	fmt.Println(replay)
		check.CheckError(err, "RPC error ObtenerEstadoRemoto")
		err = master.CallTimeout("NodoRaft.SometerOperacionRaft", &argumento, &replay, 2000*time.Millisecond)
		check.CheckError(err, "Main calltimeout")
		//out := client.Call("NodoRaft.SometerOperacionRaft", &arg, &replay)
		//	fmt.Println(out)

		fmt.Println(replay.ValorADevolver)

		//	fmt.Println("Arith: %d*%d=%d", arg.A, arg.B, replay)
	} else {
		fmt.Println("Usage: go run " + args[0] + " endpointMaster operacion clave valor")
	}
}
