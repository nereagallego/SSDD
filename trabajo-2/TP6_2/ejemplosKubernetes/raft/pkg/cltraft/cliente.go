package main

import (
	"log"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/maquinaestados"
	"raft/internal/raft"
	"strconv"
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
		master = rpctimeout.HostPort("raft-0." + args[1])
		var argumento maquinaestados.TipoOperacion
		argumento.Operacion = args[2]
		argumento.Clave = args[3]
		argumento.Valor = args[4]
		//		var reply raft.EstadoRemoto
		//	log.Println("mando a ", master)
		//	err := master.CallTimeout("NodoRaft.ObtenerEstadoNodo", &raft.Vacio{}, &reply, 10*time.Millisecond)
		//	fmt.Println(replay)
		//	check.CheckError(err, "RPC error ObtenerEstadoRemoto")
		fin := false
		for !fin {
			log.Println("mando a ", master)
			err := master.CallTimeout("NodoRaft.SometerOperacionRaft", &argumento, &replay, 2000*time.Millisecond)
			check.CheckError(err, "Main calltimeout")
			if replay.EsLider {
				fin = true
			} else {
				log.Println("No era el lider")
				master = rpctimeout.HostPort("raft-" + strconv.Itoa(replay.IdLider) + "." + args[1])
			}
		}
		//out := client.Call("NodoRaft.SometerOperacionRaft", &arg, &replay)
		//	fmt.Println(out)

		log.Println(replay.ValorADevolver)

		//	fmt.Println("Arith: %d*%d=%d", arg.A, arg.B, replay)
	} else {
		log.Println("Usage: go run " + args[0] + " dnsMaster operacion clave valor")
	}
}
