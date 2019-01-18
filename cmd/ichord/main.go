package main

import (
	"log"
	"net"

	"github.com/deepdive7/icfg"

	"github.com/deepdive7/ichord"
	"github.com/google/uuid"
)

func fatalErr(err error) {
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func main() {
	icfg.StringVar("host", "127.0.0.1", "host ip")
	icfg.IntVar("port", 9001, "listen port")
	icfg.StringVar("name", "node-1", "node name")
	icfg.StringVar("in_node", "127.0.0.1:9001", "internal node host info")
	icfg.Parse()
	ichord.DefaultConfig()

	node := ichord.NewNode(uuid.New(), icfg.String("host"), icfg.Int("port"))
	chord := ichord.NewChord(node, map[string]interface{}{})
	addr, err := net.ResolveTCPAddr("tcp", node.String())
	fatalErr(err)
	listener, err := net.ListenTCP("tcp", addr)
	fatalErr(err)
	go chord.Listen(listener)
	logApp := ichord.NewLogApp()
	err = chord.InstallApp(nil, logApp)
	fatalErr(err)
	db := ichord.NewMemKvDb("defaultDB")
	chord.SetDefaultDB(db)
	fatalErr(chord.Join())
}
