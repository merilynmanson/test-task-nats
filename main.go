package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

func Block() {
	w := sync.WaitGroup{}
	w.Add(1)
	w.Wait()
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestId    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtId      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmId        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUid          string   `json:"order_uid"`
	TrackNumber       string   `json:"track_number"`
	Entry             string   `json:"entry"`
	Delivery          Delivery `json:"delivery"`
	Payment           Payment  `json:"payment"`
	Items             []Item   `json:"items"`
	Locale            string   `json:"locale"`
	InternalSignature string   `json:"internal_signature"`
	CustomerId        string   `json:"customer_id"`
	DeliveryService   string   `json:"delivery_service"`
	Shardkey          string   `json:"shardkey"`
	SmId              int      `json:"sm_id"`
	DateCreated       string   `json:"date_created"`
	OofShard          string   `json:"oof_shard"`
}

// Метод инициализирует структуру из []byte представляющих JSON.
// Если JSON не соответствует структуре по типам, то
// метод вернет ошибку с информацией о несоответствующем типе.
func (o *Order) InitFromJSON(jsonData []byte) error {
	err := json.Unmarshal(jsonData, o)
	if err != nil {
		fmt.Println(err, "can not initialize from raw json")
	}
	return err
}

// Возвращает map, где ключ - это строка слайса, а значение -
// это количество таких строк в слайсе.
func countStringsInSlice(slice []string) map[string]int {
	result := make(map[string]int)
	for _, val := range slice {
		result[val] += 1
	}
	return result
}

// Функция извлекает ключи из JSON строки, тип формат которой
// определяется json.Marshal.
func getJsonKeysFormString(jsonString string) []string {
	var allKeys []string
	var key string
	keyStarted := false
	runes := []rune(jsonString)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '"' {
			if !keyStarted {
				keyStarted = true
			} else {
				if runes[i+1] == ':' {
					allKeys = append(allKeys, key)
				}
				key = ""
				keyStarted = false
			}
		} else {
			if keyStarted {
				key += string(runes[i])
			}
		}
	}
	return allKeys
}

// Метод стравнивает JSON структуры и переданный в функцию JSON
// в виде []bytes. Если количество ключей и имена ключей совпадают, то метод
// вернет true, иначе - false.
func (o *Order) CompareToJSON(jsonData []byte) bool {
	orderJson, err := json.Marshal(o)
	if err != nil {
		fmt.Println(err, "can not marshal structure")
		return false
	}

	orderKeys := getJsonKeysFormString(string(orderJson))
	jsonKeys := getJsonKeysFormString(string(jsonData))

	orderKeysCount := countStringsInSlice(orderKeys)
	jsonKeysCount := countStringsInSlice(jsonKeys)

	for key, number := range jsonKeysCount {
		orderValue, ok := orderKeysCount[key]
		if !ok {
			return false
		}
		if orderValue != number {
			return false
		}
	}
	return true
}

type Cache struct {
	Orders map[string][]byte
}

// Метод получет все записи orders из базы данных db в map кэша.
func (c *Cache) RestoreFromDb(db *Db) error {
	var err error
	c.Orders, err = db.GetAllOrders()
	if err != nil {
		fmt.Println(err, "can not initialize from raw json")
	}
	fmt.Println("RESTORED FROM DB: ", c.Orders)
	return nil
}

// Метод создаёт новый объект кэша и возвращает ссылку на него.
func NewCache(db *Db) (*Cache, error) {
	var cache *Cache = &Cache{}
	cache.Orders = make(map[string][]byte)
	err := cache.RestoreFromDb(db)
	if err != nil {
		fmt.Println(err, "can not restore cache from db")
		return nil, err
	}
	return cache, err
}

// Метод добавляет заказ в кэш с ключом uid и значением newItem.
// Если элемент уже существует в map Orders, то вернет ошибку.
func (c *Cache) AddOrder(uid string, newItem []byte) error {
	_, ok := c.Orders[uid]
	if ok {
		errorMsg := fmt.Sprintf("Element %s already exists in cache", uid)
		return errors.New(errorMsg)
	}
	c.Orders[uid] = newItem
	return nil
}

// Метод получает элемент по ключу uid из map Orders. Если элемент
// найден, то вернется его значение, иначе - nil.
func (c *Cache) GetOrder(uid string) []byte {
	value, ok := c.Orders[uid]
	if ok {
		return value
	}
	return nil
}

type Db struct {
	dataBase *sql.DB
}

// Метод открывает соединение с базой данных с драйвером БД driverName и
// данными для соединения с БД connectionData (например, содержит пароль, порт).
func (db *Db) open(driverName string, connectionData string) error {
	var err error
	db.dataBase, err = sql.Open(driverName, connectionData)
	if err != nil {
		fmt.Printf("Can not connect to DB: %v\n", err)
	}
	return err
}

// Метод создаёт и инициализирует новый объект Db с драйвером БД driverName и
// данными для соединения с БД connectionData (например, содержит пароль, порт).
func NewDb(driverName string, connectionData string) (*Db, error) {
	var db *Db = &Db{}
	err := db.open(driverName, connectionData)
	if err != nil {
		fmt.Println(err, "can not open db connection")
	}
	return db, err
}

// Метод добавляет запись order в БД со столбцами uid(uid) и json_data(newOrder).
func (db *Db) AddOrder(uid string, newOrder []byte) error {
	row := db.dataBase.QueryRow("INSERT INTO orders VALUES ($1, $2)", uid, string(newOrder))
	return row.Err()
}

// Метод получает json_data из таблицы orders по uid заказа.
func (db *Db) GetOrder(uid string) ([]byte, error) {
	row := db.dataBase.QueryRow("SELECT json_data FROM orders WHERE uid='$1'", uid)
	err := row.Err()
	if err != nil {
		fmt.Println(err, "can not get order row from db")
		return nil, err
	}
	var orderData []byte
	err = row.Scan(orderData)
	if err != nil {
		fmt.Println(err, "can not scan order from row")
		return nil, err
	}
	return orderData, err
}

// Метод получает все записи таблицы orders, помещая в map и возвращая эту map.
func (db *Db) GetAllOrders() (map[string][]byte, error) {
	orders := make(map[string][]byte)
	rows, err := db.dataBase.Query("SELECT * FROM orders;")
	if err != nil {
		fmt.Println(err, "can not get orders rows from db")
		return nil, err
	}
	for rows.Next() {
		var uid string
		var jsonData []byte
		err = rows.Scan(&uid, &jsonData)
		if err != nil {
			fmt.Println(err, "can not scan order row")
			return nil, err
		}
		orders[uid] = jsonData
	}
	return orders, err
}

// Метод закрывает соединение с БД.
func (db *Db) CloseConn() {
	err := db.dataBase.Close()
	if err != nil {
		fmt.Println(err, "can not close db connection")
	}
}

type NatsStreamingConn struct {
	conn stan.Conn
}

// Метод создает объект соединения с nats-steaming и возвращает ссылку на него.
// Подключение создаётся по clusterID и clientID.
func NewNatsStreamingConn(clusterId string, clientId string) (
	*NatsStreamingConn,
	error,
) {
	var natsStreamingConn *NatsStreamingConn = &NatsStreamingConn{}
	var err error
	natsStreamingConn.conn, err = stan.Connect(clusterId, clientId)
	if err != nil {
		fmt.Println(err, "can not open connection to nats-streaming")
	}
	return natsStreamingConn, err
}

// Метод подписывается на тему subject c обработчиком полученных сообщений handler.
func (n *NatsStreamingConn) Subscribe(
	subject string,
	handler stan.MsgHandler,
) {
	_, err := n.conn.Subscribe(subject, handler, stan.DurableName("sub-1-durable"))
	if err != nil {
		fmt.Println(err, "can not subscribe on ", subject)
	}
}

// Метод закрывает соединение с nats-streaming.
func (n *NatsStreamingConn) Close() error {
	return n.conn.Close()
}

type OrdersServer struct {
	Host   string
	Port   string
	router *mux.Router
	cache  *Cache
	db     *Db
}

// Метод создаёт и инициализирует сервер для http запросов для работы с заказами.
func NewOrdersServer(host string, port string, cache *Cache, db *Db) *OrdersServer {
	return &OrdersServer{
		Host:   host,
		Port:   port,
		router: mux.NewRouter(),
		cache:  cache,
		db:     db,
	}
}

// Метод устанавливает обработчик для метода GET для сервера.
// Использует пакет gorilla/mux.
func (s *OrdersServer) Setup() {
	s.router.HandleFunc("/orders/{uid}", func(w http.ResponseWriter, r *http.Request) {
		var err error
		vars := mux.Vars(r)
		var uid string = vars["uid"]
		var order []byte = s.cache.GetOrder(uid)
		if order == nil {
			order, err = s.db.GetOrder(uid)
		}
		if err != nil {
			fmt.Printf("Can not get order: %v\n", err)
		}
		_, err = w.Write(order)
		if err != nil {
			fmt.Printf("Can not write response: %v\n", err)
		}
	}).Methods("GET")
}

// Метод переводит сервер на прослушивание http запросов.
// Использует стандартый пакет net/http.
func (s *OrdersServer) Run() {
	go http.ListenAndServe(s.Host+":"+s.Port, s.router)
}

type Program struct {
	natsConn *NatsStreamingConn
	db       *Db
	cache    *Cache
}

// Метод инициализирует объект Program. Создает кэш, БД, соединение с nats-streaming,
// http-server.
func (p *Program) Setup() error {
	var err error

	p.db, err = NewDb(
		"postgres",
		"dbname=orders user=postgres password=qwerty sslmode=disable",
	)
	if err != nil {
		fmt.Printf("Can not create DB connection: %v\n", err)
	}

	p.cache, err = NewCache(p.db)

	p.natsConn, err = NewNatsStreamingConn("test-cluster", "sub-1")
	if err != nil {
		fmt.Printf("Can not create nats-streaming connection: %v\n", err)
	}
	p.natsConn.Subscribe("orders", func(msg *stan.Msg) {
		var newOrder Order
		err = newOrder.InitFromJSON(msg.Data)
		if err != nil {
			fmt.Println("Can not initialize order from JSON: ", err)
		}
		if newOrder.CompareToJSON(msg.Data) && err == nil {
			p.db.AddOrder(newOrder.OrderUid, msg.Data)
			p.cache.AddOrder(newOrder.OrderUid, msg.Data)
		}

	})
	server := NewOrdersServer("127.0.0.1", "8081", p.cache, p.db)
	server.Setup()
	server.Run()
	return err
}

// Метод закрывает все соединения, созданные в Program.
func (p *Program) CloseConnections() {
	p.db.CloseConn()
	p.natsConn.Close()
}

func main() {
	program := Program{}
	err := program.Setup()
	if err != nil {
		fmt.Println(err, "can not run Program")
	}
	defer program.CloseConnections()
	Block()
}
