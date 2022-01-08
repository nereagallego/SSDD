/*
Cliente que interacciona con servidor prueba de 2 formas:
    - De forma automatica con intercambio de 1 mensaje
         - (solo necesita 1 argumento, dirección propia)
    - En interactivo (2 parámetros , 1º como anterior, 2º cualquiera)
*/
package main

import (
    "fmt"
    "conversa/msgsys"
    "os"
)



func main() {
	fmt.Print("Arrancando cliente: ")

	server := "s1.prueba.default.svc.cluster.local:6000"
	
	switch len(os.Args) {
	case 2: //comunicacion automatica con @ DNS de sevidor conocida a priori
	    me := os.Args[1]  // host.puerto del cliente
	    commAutomatico(me, server)
	    
	case 3: //comunicacion interactiva con servidor
	    me := os.Args[1]
	    commInteractiva(me, server)
	    
	default: fmt.Println("No se puede ejecutar sin 1 or 2 argumentos")
    }
}

func commAutomatico(me, server string) {
    fmt.Println(me)
    
    ms := msgsys.StartMsgSys(msgsys.HostPuerto(me))
    
    // enviar....
    menEnviar := msgsys.
        Message{Contenido: "Probando ....", Remitente: ms.Me()}
    ms.Send(msgsys.HostPuerto(server), menEnviar)
    
    // Y recibir respuesta y visualizar
    menRecib := ms.Receive()
    fmt.Println("Recibido : ", menRecib.Contenido)
}

func commInteractiva(me, server string) {
    fmt.Println(me)
    
    ms := msgsys.StartMsgSys(msgsys.HostPuerto(me))
    
    // iteración interactiva de envios y recepciones
    var menRecib msgsys.Message
    menEnviar := msgsys.Message{Remitente: ms.Me()}
	for {
	    fmt.Print("Que quieres enviar ? ")
	    fmt.Scanln(&menEnviar.Contenido)
	    ms.Send(msgsys.HostPuerto(server), menEnviar)
	    menRecib = ms.Receive()
	    fmt.Println("Recibido : ", menRecib.Contenido)
	    if menEnviar.Contenido == "by" { return }
	}
}
