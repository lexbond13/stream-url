package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
	"bitbucket.org/lexbond/stream-url/url"
	"github.com/go-redis/redis"
	"github.com/jinzhu/configor"
	"net"
	"google.golang.org/grpc"
)

const (
	port = ":50052"
)

type server struct {

}

var (
	config   Config
	redisCli *redis.Client
	checkedUrl string
)

type Config struct {
	APPName          string
	Urls             []string
	MinTimeout       int
	MaxTimeout       int
	NumberOfRequests int
}

type ResponseData struct {
	Data   string
	Source string
}

func Init() {
	configor.Load(&config, "config.yml")
	redisCli = initRedis()
}

func (s *server) GetRandomDataStream(empty *url.Empty, stream url.Url_GetRandomDataStreamServer) error {
	runRoutines(stream)

	return nil
}

func main() {
	Init()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("failed to listen ", err)
	}

	s := grpc.NewServer()
	url.RegisterUrlServer(s, &server{})
	s.Serve(lis)
}


func runRoutines(stream url.Url_GetRandomDataStreamServer) {

	responsesReceiver := make(chan ResponseData)
	done := make(chan interface{})

	waitGroup := sync.WaitGroup{}

	var mutex sync.Mutex

	for i := 0; i < config.NumberOfRequests; i++ {
		position := randInt(0, len(config.Urls))
		waitGroup.Add(1)

		go getRandomUrl(config.Urls[position], &waitGroup, responsesReceiver, &mutex)
	}

	go func(wg *sync.WaitGroup, ch chan interface{}) {
		wg.Wait()
		close(ch)
	}(&waitGroup, done)

	for {
		select {
		case data := <-responsesReceiver:
			dataResponse := &url.UrlData{}
			dataResponse.Data = data.Source
			stream.Send(dataResponse)

		case <-done:
			close(responsesReceiver)
			return
		}
	}
}

func getRandomUrl(url string, wg *sync.WaitGroup, responseCh chan<- ResponseData , mutex *sync.Mutex) {
	defer wg.Done()

	mutex.Lock()
	checkedUrl = getMD5Hash(url)

	if val, _ := redisGet(checkedUrl); val!="" {
		responseCh <- ResponseData{Data:val, Source:"from cache "+url}
	} else {
		//если данных в кэше нет, проверяем есть ли лок
		if !existLock(checkedUrl) {
			//если лока нет, ставим его и загружаем данные
			redisSetLock(checkedUrl)
			data, err := getDataByUrl(url)
			if err == nil {
				redisSet(checkedUrl, string(data))
				responseCh <- ResponseData{Data: string(data), Source: "from url "+ url}
			}
			// снимаем лок даже если ответа нет
			redisSetUnlock(checkedUrl)
		}

	}
	mutex.Unlock()
}

func getDataByUrl(url string) ([]byte, error) {

	client := http.Client{Timeout: 10 * time.Second}
	req, err := client.Get(url)
	if err != nil {
		log.Printf("Не могу получить данные по адресу %v, ошибка: %v", url, err)
		return nil, err
	}
	defer req.Body.Close()

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Не могу получить код страницы по адресу %v, ошибка: %v", url, err)
		return nil, err
	}

	return b, nil
}

func getMD5Hash(str string) string {
	hashKey := md5.New()
	hashKey.Write([]byte(str))
	return hex.EncodeToString(hashKey.Sum(nil))
}

func randInt(min int, max int) int {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Intn(max-min)
}
