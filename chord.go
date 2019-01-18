package ichord

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bluele/gcache"

	"github.com/deepdive7/icfg"
	"github.com/deepdive7/ilog"
	"github.com/deepdive7/iveil"
)

var (
	publicNodes []*Node
)

var (
	chordLogger ilog.Logger
)

func init() {
	chordLogger = ilog.NewSimpleDefaultLogger(os.Stdout, ilog.DEBUG, "ichord->", true)
}

func DefaultConfig() {
	icfg.SetDefaultKey("host", "127.0.0.1")
	icfg.SetDefaultKey("port", 9001)
	icfg.SetDefaultKey("max_copies", 3)
	icfg.SetDefaultKey("callback_cache_size", 128)
	icfg.SetDefaultKey("hash_circle_cache_size", 128)
	icfg.SetDefaultKey("node_state_cache_size", 128)
}

type ChordAPI interface {
	//Encode and decode the message
	Encode(dataBuf *bytes.Buffer, v interface{}) error
	Decode(dataBuf *bytes.Buffer, v interface{}) error

	//Operate the default db, default is map
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error

	//Set to a specify db
	DBSet(dbName string, key, value []byte) error
	// Get from a specify db
	DBGet(dbName string, key []byte) ([]byte, error)
	//Del from a specify db
	DBDel(dbName string, key []byte) error

	// set default db to replace the default map
	SetDefaultDB(db KvDb)
	//add new kv db
	RegisterDB(dbName string, db KvDb)
	//close a db and remove from list
	CloseDB(dbName string)
	//close a db and remove it, it's dangerous
	DropDB(dbName string)

	//Install an app to handle messages
	InstallApp(cfg *icfg.Config, app Application) error
	//Uninstall an app
	UninstallApp(id UUID) error

	Node() Node
	//Ping is used to detect a node is connectable or not
	Ping(node *Node) bool
	//SendTo a node with []byte format data
	SendTo(node *Node, data []byte) error
	//SendMsg a message and find receiver from the message
	SendMsg(msg *Message) (*Message, error)

	//Listen will hold on a listener and accept connections then handle it.
	Listen(l net.Listener) error
	// maintain will periodically perform maintenance operations
	Maintain()
	//Sync is used to check local data replications number periodically and if not then send that data to other nodes
	Sync()
	//Count statistics how many nodes in the network
	Count() int

	//Join will use self uuid to ask specify nodes or build-in public node to initialize node
	Join(node ...*Node) error
	//Leave tells the specify node or the public nodes I will offline, update network states and re-sync data
	Leave(node ...*Node) error
	//Route find the node that stored this key or should store this key
	Route(key UUID) (*Node, error)
	//FindSuccessor returns the first successor for the given key
	FindSuccessor(key UUID) (*Node, error)
	//FindPredecessor returns the first predecessor for the given key
	FindPredecessor(key UUID) (*Node, error)

	//List returns an array contains all stored nodes' info
	List() ([]*Node, error)
	//Stabilize successor/predecessor pointers
	Stabilize() error
	//Notify a Node of our existence
	Notify(n *Node) (*Message, error)
	//FixFingers table
	FixFingers()
	//ShowFingers table
	ShowFingers() []Finger
}

// Create a new Cluster
func NewChord(self *Node, args map[string]interface{}) *Chord {
	hasher := iveil.NewBlake2b()

	return &Chord{
		self:      self,
		kill:      make(chan bool),
		maxCopies: icfg.Int("max_copies"),

		apps:             make(map[UUID]Application),
		leftFingerTable:  &FingerTable{},
		rightFingerTable: &FingerTable{},
		numFingers:       hasher.Size(),
		conn:             make(chan net.Conn),

		coder:           NewCoder("json"),
		hasher:          hasher,
		dbMutex:         &sync.Mutex{},
		dbs:             make(map[string]KvDb),
		callbackCache:   gcache.New(icfg.Int("callback_cache_size")).Build(),
		hashCircleCache: gcache.New(icfg.Int("hash_circle_cache_size")).Build(),
		nodeStateCache:  gcache.New(icfg.Int("node_state_cache_size")).Build(),

		stabilizeMin:  time.Second * 5,
		stabilizeMax:  time.Second * 10,
		heartbeatFreq: time.Second * 10,
	}
}

type Chord struct {
	self      *Node
	midHash   UUID
	kill      chan bool
	maxCopies int

	apps             map[UUID]Application
	leftFingerTable  *FingerTable
	rightFingerTable *FingerTable
	numFingers       int
	conn             chan net.Conn

	coder           Coder
	hasher          *iveil.Blake2b
	defaultDb       KvDb
	dbMutex         *sync.Mutex
	dbs             map[string]KvDb
	callbackCache   gcache.Cache
	hashCircleCache gcache.Cache
	nodeStateCache  gcache.Cache

	stabilizeMin  time.Duration
	stabilizeMax  time.Duration
	heartbeatFreq time.Duration
}

func (c *Chord) Set(key, value []byte) error {
	//每个保存一份
	return nil
}

//Set，Get，Del操作首先令k=hash(key), 生成maxCopies个hash, (k+i*maxHash/maxCopies) mod maxHash, 找出负责节点,去重,并存入
//hashCircleCache里，过期时间从配置中读取
func (c *Chord) Get(key []byte) ([]byte, error) {
	//依次请求，超时则下一个
	return nil, nil
}

func (c *Chord) Del(key []byte) error {
	//每个发送删除请求
	return nil
}

func (c *Chord) DBSet(dbName string, key, value []byte) error {
	//每个保存一份
	return c.dbs[dbName].Set(key, value)
}

func (c *Chord) DBGet(dbName string, key []byte) ([]byte, error) {
	//依次请求，超时则下一个
	return c.dbs[dbName].Get(key)
}

func (c *Chord) DBDel(dbName string, key []byte) error {
	//每个发送删除请求
	return c.dbs[dbName].Del(key)
}

func (c *Chord) SetDefaultDB(db KvDb) {
	c.defaultDb = db
}

func (c *Chord) RegisterDB(dbName string, db KvDb) {
	//不主动拉取数据，等待其他节点检查数据备份不足时找到自己再存
	c.dbMutex.Lock()
	defer c.dbMutex.Unlock()
	c.dbs[dbName] = db
}

func (c *Chord) CloseDB(dbName string) {
	//主动检查数据备份，丢到Successor
	c.dbMutex.Lock()
	defer c.dbMutex.Unlock()
	if _, ok := c.dbs[dbName]; ok {
		c.dbs[dbName].Close()
	}
}

func (c *Chord) DropDB(dbName string) {
	//不同步，本方法在收到db创建者删除请求时才会执行
	c.dbMutex.Lock()
	defer c.dbMutex.Unlock()
	if _, ok := c.dbs[dbName]; ok {
		c.dbs[dbName].Drop()
	}
}

func (c *Chord) InstallApp(cfg *icfg.Config, app Application) error {
	//消息相关广播到各个app处理
	c.apps[app.ID()] = app
	return app.Startup(cfg)
}

func (c *Chord) UninstallApp(id UUID) error {
	//卸载app
	err := c.apps[id].ShutDown()
	delete(c.apps, id)
	return err
}

func (c *Chord) SendTo(node *Node, data []byte) error {
	//将[]byte封装进message后SendMsg发送
	panic("implement me")
}

// SendMsg a message through the network to it's intended Node
func (c *Chord) SendMsg(msg *Message) (*Message, error) {
	//根据msg的target信息选择或者创建连接
	Debug("Sending message with key %v", msg.key)
	// find the appropriate node in our list of known nodes
	target, err := c.Route(msg.key)
	if err != nil {
		c.throwErr(err)
		return nil, err
	}
	if Equal(target.Id, c.self.Id) {
		if msg.purpose > PreReq {
			c.onDeliver(msg)
		}
		return nil, nil
	}

	// decide if our application permits the message
	if c.forward(msg, target) {
		// send the message
		resp, err := gPool.SRMsg(target.Id, msg)
		if err != nil {
			c.throwErr(err)
			return nil, err
		} else if resp.purpose == StatusError {
			c.throwErr(err)
			return resp, errors.New(string(resp.body))
		}
		return resp, nil
	}
	return nil, nil
}

func (c *Chord) Join(nodes ...*Node) error {
	//向nodes列表发送加入请求，得到自己的node,定时并少数服从多数，相同个数选第一个，没有nodes则发送给public nodes
	var preNodes []*Node
	if len(nodes) > 0 {
		preNodes = nodes
	} else {
		preNodes = publicNodes
	}
	for _, n := range preNodes {
		Debug(n.String())
	}
	var newNode *Node
	c.self = newNode
	return nil
}

func (c *Chord) Leave(nodes ...*Node) error {
	//向nodes列表发送离开请求，没有nodes则发送给public nodes，这里需要close所有db,关闭连接池,关闭listener
	var preNodes []*Node
	if len(nodes) > 0 {
		preNodes = nodes
	} else {
		preNodes = publicNodes
	}
	for _, n := range preNodes {
		Debug(n.String())
	}
	return nil
}

func (c *Chord) Listen(ln net.Listener) error {
	//accept连接后交给handleConn,收到kill信号时退出协程

	go func() {
		defer handleErr(ln.Close)
		for {
			conn, err := ln.Accept()
			if err != nil {
				c.throwErr(err)
				return
			}
			Debug("Received connection from ", conn.RemoteAddr().String())
			c.conn <- conn
		}
	}()

	go func() {
		for {
			select {
			case <-c.kill:
				return
			case <-time.After(c.heartbeatFreq):
				Debug("Sending heartbeats")
				//go c.sendHeartbeats()

			// Run the stabilize routine randomly between stabilizeMin and stabilizeMax
			case <-time.After(time.Duration(randRange(c.stabilizeMin.Nanoseconds(), c.stabilizeMax.Nanoseconds()))):
				Debug("Running stabilize routine")
				go handleErr(c.Stabilize)
			case conn := <-c.conn:
				Debug("Handling connection")
				go c.handleConn(conn)
			}
		}
	}()
	return nil
}

func (c *Chord) Node() Node {
	return *(c.self)
}

func (c *Chord) Ping(node *Node) bool {
	//发送一个ping请求，构造一个callback channel, 设置等待超时，配置读取
	panic("implement me")
}

func (c *Chord) Encode(dataBuf *bytes.Buffer, v interface{}) error {
	return c.coder.NewEncoder(dataBuf).Encode(v)
}

func (c *Chord) Decode(dataBuf *bytes.Buffer, v interface{}) error {
	return c.coder.NewDecoder(dataBuf).Decode(v)
}

func (c *Chord) Maintain() {
	//检查数据副本数，小于最大副本数时会计算hashCircle请求备份，定期ping检查状态等，维护网络
	panic("implement me")
}

// TODO Sync reserved
func (c *Chord) Sync() {
	//从其他节点拉取节点数据库信息，其他数据库不同步，如果Join自定义节点，则会加入该节点所有的数据库
	panic("implement me")
}

// TODO Count calculate total count of nodes in the network
func (c *Chord) Count() int {
	//发送统计请求获取网络中节点个数，不断发给Successor,每次加一，将整个hashCircle切分成8份，最后加和广播出去.
	//每个节点会把个数缓存到cache中，有效期从配置读取，cache读不到才会重新统计。
	panic("implement me")
}

func (c *Chord) Route(key UUID) (*Node, error) {
	//找到负责存储key的节点，如果大于midHash,逆时针找，小于则顺时针找，维护了两个方向的finger table
	//当key位于前驱和自己之间，那这个key是自己负责的
	Debug("Determining route to the given NodeID: %v", key)
	// check if we are responsible for the key
	if betweenRightInc(c.self.predecessor.Id, c.self.Id, key) {
		Debug("I'm the target. Delivering message %v", key)
		return c.self, nil
	}
	//当key位于自己和后继之间，那这个key是后继负责的
	// check if our successor is responsible for the key
	if betweenRightInc(c.self.Id, c.self.successor.Id, key) {
		// our successor is the desired node
		Debug("Our successor is the target. Delivering message %s", key)
		return c.self.successor, nil
	}

	// finally check if one our fingers is closer to (or is) the desired node
	Debug("Checking fingerTable for target node...")
	return c.FindSuccessor(key)
}

func (c *Chord) FindSuccessor(key UUID) (*Node, error) {
	//从顺时针finger table开始寻找，找到hash大于key hash的所有节点发送请求，设置超时时间，
	// 时间为请求ttl*5s，超时则向下一个节点发送，发送时ttl-1
	Debug("Finding successor to key %v", key)
	request := NewMessage(c.self, SucReq, key, nil)
	response, err := c.SendMsg(request)
	if err != nil {
		return nil, err
	}

	if response.purpose == StatusError {
		return nil, errors.New(string(response.body))
	}

	var node *Node
	err = json.Unmarshal(response.body, &node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *Chord) FindPredecessor(key UUID) (*Node, error) {
	//从逆时针finger table开始寻找，找到hash小于key hash的所有节点发送请求，设置超时时间，
	// 时间为请求ttl*5s，超时则向下一个节点发送，发送时ttl-1
	Debug("Finding closest node in our finger table to node %v", key)
	prev := c.self
	for _, finger := range c.leftFingerTable.table {
		if betweenRightInc(prev.Id, finger.node.Id, key) {
			return finger.node, nil
		}
		prev = finger.node
	}
	return nil, NewError("No node exists for given id")
}

func (c *Chord) List() ([]*Node, error) {
	//从左右finger table取出node并去重，还有hashCircleCache中的节点
	panic("implement me")
}

func (c *Chord) Stabilize() error {
	//检查更新前驱后继
	Debug("stabilizing...")
	// craft message for pred_req
	predReq := NewMessage(c.self, PreReq, c.self.successor.Id, nil)
	data, err := json.Marshal(predReq)
	_, err = gPool.SendTo(c.self.successor.Id, data)
	if err != nil {
		return err
	}

	var predecessor *Node
	body, err := gPool.ReadFrom(c.self.successor.Id)
	err = c.Decode(bytes.NewBuffer(body), predecessor)

	// check if our sucessor has a diff predecessor then us
	// if so notify the new successor and update our own records
	if Equal(c.self.Id, predecessor.Id) {
		Debug("Found new predecessor! Id: %v", predecessor.Id)
		resp, err := c.Notify(predecessor)
		if err != nil {
			return err
		} else if resp.purpose == StatusError {
			return errors.New(string(resp.body))
		}

		c.self.successor = predecessor
	}

	return nil
}

func (c *Chord) Notify(n *Node) (*Message, error) {
	//通知所有连接节点即将下线
	Debug("Notifying node: %v of our existence", n.Id)
	ann := NewMessage(c.self, NodeNotify, n.Id, nil)
	return gPool.SRMsg(n.Id, ann)
}

func (c *Chord) FixFingers() {
	//Periodically check finger table
	panic("implement me")
}

func (c *Chord) ShowFingers() []Finger {
	//return finger table
	panic("implement me")
}

/*
Internal Methods
*/

func (c *Chord) newSock(conn net.Conn) *sock {
	//conn.SetNoDelay(true)
	//conn.SetKeepAlive(true)
	return &sock{
		host: conn.RemoteAddr().String(),
		conn: conn,
		enc:  c.coder.NewEncoder(conn),
		dec:  c.coder.NewDecoder(conn),
	}
}

// Handle new connections
func (c *Chord) handleConn(conn net.Conn) {
	Debug("Received a new connection")

	sock := c.newSock(conn)
	defer handleErr(conn.Close)
	var msg *Message
	var resp *Message
	var err error

	if err := sock.read(msg); err != nil {
		c.throwErr(err)
		return
	}
	Debug("Received a message with purpose %v from node %v", msg.purpose, msg.sender)
	msg.hops++

	switch msg.purpose {
	// A node wants to join the network
	// we need to find his appropriate successor
	case NodeJoin:
		resp, err = c.onNodeJoin(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// A node wants to leave the network
		// so update our finger table accordingly
	case NodeLeave:
		break

		// Received a heartbeat message from a connected
		// client, let them know were still alive
	case HeartBeat:
		resp = c.onHeartBeat(msg)

		// We've been notified of a new predecessor
		// node, so update our fingerTable
	case NodeNotify:
		resp, err = c.onNotify(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Received a successor request from a node,
	case SucReq:
		resp, err = c.onSuccessorRequest(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Received a predecessor request from a node
	case PreReq:
		resp, err = c.onPredecessorRequest(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Not an internal message, forward or deliver it
	default:
		resp = statusOKMessage(c.self, msg.sender.Id)
		c.SendMsg(msg)
	}

	sock.write(resp)
}

//////////////////////////////////////////////
//											//
//  Internal Chord Application handlers 	//
//											//
//////////////////////////////////////////////

func (c *Chord) onDeliver(msg *Message) {
	Debug("Delivering message to registered applications")
	for _, app := range c.apps {
		app.OnDeliver(msg)
	}
}

func (c *Chord) onHeartBeat(msg *Message) *Message {
	Debug("Received heartbeat message from node %v", msg.sender.Id)
	for _, app := range c.apps {
		go app.OnHeartbeat(msg.sender)
	}
	return NewMessage(c.self, StatusOk, msg.sender.Id, nil)
}

func (c *Chord) onNodeJoin(msg *Message) (*Message, error) {
	Debug("Received node join message from node %v", msg.sender.Id)
	for _, app := range c.apps {
		go app.OnNodeJoin(msg.sender)
	}
	req := NewMessage(c.self, SucReq, msg.sender.Id, nil)
	return c.SendMsg(req)
}

// Called when a NodeLeave message is received
func (c *Chord) onNodeLeave(msg *Message) {
	for _, app := range c.apps {
		go app.OnNodeExit(msg.sender)
	}
}

func (c *Chord) onNotify(msg *Message) (*Message, error) {
	Debug("Node %v is notifying us of its existence", msg.sender.Id)
	err := msg.DecodeBody(c.coder, &c.self.predecessor)
	if err != nil {
		return statusErrMessage(c.self, msg.sender.Id, err), err
	}
	return statusOKMessage(c.self, msg.sender.Id), nil
}

// Handle a successor request we've received
func (c *Chord) onSuccessorRequest(msg *Message) (*Message, error) {
	Debug("Received successor request from node %v", msg.sender.Id)
	if c.self.IsResponsible(msg.target.Id) {
		// send successor
		buf := bytes.NewBuffer(make([]byte, 0))
		err := c.Encode(buf, c.self.successor)
		if err != nil {
			return statusErrMessage(c.self, msg.sender.Id, err), err
		}
		return NewMessage(c.self, SucReq, msg.sender.Id, buf.Bytes()), nil
	} else {
		// forward it on
		go c.SendMsg(msg)
		return statusOKMessage(c.self, msg.sender.Id), nil
	}
}

func (c *Chord) onPredecessorRequest(msg *Message) (*Message, error) {
	Debug("Received predecessor request from node: %v", msg.sender.Id)
	if c.self.IsResponsible(msg.target.Id) {
		// send successor
		buf := bytes.NewBuffer(make([]byte, 0))
		err := c.Encode(buf, c.self.predecessor)
		if err != nil {
			return statusErrMessage(c.self, msg.sender.Id, err), err
		}
		return NewMessage(c.self, PreReq, msg.sender.Id, buf.Bytes()), nil
	} else {
		// forward it on
		go c.SendMsg(msg)
		return statusOKMessage(c.self, msg.sender.Id), nil
	}
}

// Decide whether or not to continue forwarding the message through the network
func (c *Chord) forward(msg *Message, next *Node) bool {
	Debug("Checking if we should forward the given message")
	forward := true

	for _, app := range c.apps {
		forward = forward && app.OnForward(msg, next)
	}

	return forward
}

// Handle any cluster errors
func (c *Chord) throwErr(err error) {
	Error(err.Error())
	// SendMsg the error through all the embedded apps
	for _, app := range c.apps {
		app.OnError(err)
	}
}
