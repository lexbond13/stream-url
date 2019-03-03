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
	urlProto "bitbucket.org/lexbond/stream-url/url"
	"github.com/go-redis/redis"
	"github.com/jinzhu/configor"
	"net/url"
	"net"
	"google.golang.org/grpc"
)

const (
	port = ":50052"
)

type server struct {
}

var (
	config    Config
	redisCli  *redis.Client
	validUrls []*url.URL
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

	validUrls = validateURLs(config.Urls)
}

func main() {
	Init()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("failed to listen ", err)
	}

	s := grpc.NewServer()
	urlProto.RegisterUrlServer(s, &server{})
	s.Serve(lis)
}

func validateURLs(urlForValidate []string) []*url.URL {
	URLs := []*url.URL{}

	for _, u := range urlForValidate {
		parsedURL, err := url.Parse(u)
		if err != nil {
			log.Printf("Url is not valid: %v \n", u)
			continue
		}
		URLs = append(URLs, parsedURL)
	}
	return URLs
}

func (s *server) GetRandomDataStream(empty *urlProto.Empty, stream urlProto.Url_GetRandomDataStreamServer) error {
	runRoutines(stream)

	return nil
}

func runRoutines(stream urlProto.Url_GetRandomDataStreamServer) {

	responsesReceiver := make(chan ResponseData)
	done := make(chan interface{})

	waitGroup := sync.WaitGroup{}

	var mutex sync.Mutex

	for i := 0; i < config.NumberOfRequests; i++ {
		position := randInt(0, len(validUrls))
		waitGroup.Add(1)

		go getRandomUrl(validUrls[position].String(), &waitGroup, responsesReceiver, &mutex)
	}

	go func(wg *sync.WaitGroup, ch chan interface{}) {
		wg.Wait()
		close(ch)
	}(&waitGroup, done)

	for {
		select {
		case data := <-responsesReceiver:
			dataResponse := &urlProto.UrlData{}
			dataResponse.Data = data.Source
			stream.Send(dataResponse)

		case <-done:
			close(responsesReceiver)
			return
		}
	}
}

func getRandomUrl(URL string, wg *sync.WaitGroup, responseCh chan<- ResponseData, mutex *sync.Mutex) {
	defer wg.Done()

	checkedUrl := getMD5Hash(URL)

	// TODO вынести задержки в конфг, в формате time.ParseDuration
	for _, delay := range []time.Duration{0, 1 * time.Second, 2 * time.Second} {
		if delay != 0 {
			select {
			case <-time.After(delay):
			}
		}

		if val, _ := redisGet(checkedUrl); val != "" {
			responseCh <- ResponseData{Data: val, Source: "from cache " + URL}
			return
		}

		if existLock(checkedUrl) {
			continue
		}
		getData(checkedUrl, URL, responseCh, mutex)
	}
}

func getData(checkedUrl string, URL string, responseCh chan<- ResponseData, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()

	//если лока нет, ставим его и загружаем данные
	redisSetLock(checkedUrl)
	// снимаем лок даже если ответа нет
	defer redisSetUnlock(checkedUrl)

	data, err := getDataByUrl(URL)
	if err != nil {
		// TODO сделать обработку когда URL недоступен, либо возвращает 404 или 500. В этом случае можно либо приостанавливать обработку адреса, либо исключать его
		fmt.Println(err.Error())
		return
	}
	redisSet(checkedUrl, string(data))
	responseCh <- ResponseData{Data: string(data), Source: "from url " + URL}

	// снимаем лок даже если ответа нет
	redisSetUnlock(checkedUrl)
}

func getDataByUrl(URL string) ([]byte, error) {

	client := http.Client{Timeout: 10 * time.Second}
	req, err := client.Get(URL)
	if err != nil {
		log.Printf("Не могу получить данные по адресу %v, ошибка: %v", URL, err)
		return nil, err
	}
	defer req.Body.Close()

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Не могу получить код страницы по адресу %v, ошибка: %v", URL, err)
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
