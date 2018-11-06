package main

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"flag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var bucket = flag.String("bucket", "", "S3 Bucket to copy contents from. (required)")
var concurrency = flag.Int("concurrency", 500, "Number of concurrent connections to use.")
var queueSize = flag.Int("queueSize", 3000, "Size of the queue")
var env = flag.String("env", "DEV", "Environment")

func main() {
	flag.Parse()

	if len(*bucket) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	// set up aws session and s3 client
	awsSess, err := session.NewSession(aws.NewConfig())
	if err != nil {
		log.Fatalf("Failed to create a new session. %v", err)
	}
	s3Client := s3.New(awsSess, aws.NewConfig().WithRegion("eu-central-1"))

	var redisHost string
	if *env == "DEV" {
		redisHost = ":6379"
	} else {
		redisHost = "context-staging.czhgsk.clustercfg.euc1.cache.amazonaws.com:6379"
	}
	var redisPool = newPool(redisHost) // create redis connection pool

	DownloadBucket(s3Client, *redisPool, *bucket, *concurrency, *queueSize)
}

func DownloadBucket(client *s3.S3, redisPool redis.Pool,bucket string, concurrency, queueSize int) {
	keysChan := make(chan string, queueSize)
	cpyr := &Copier{
		client: client,
		bucket: bucket,
		pool: redisPool,
	}
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysChan {
				data, err := cpyr.Download(key)
				if err != nil {
					log.Printf("Failed to download key %v, due to %v", key, err)
				}
				err = cpyr.Upload(key, data)
				if err != nil {
					log.Printf("Failed to upload key %v, due to %v", key, err)
				}
			}
		}()
	}

	req := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}
	err := client.ListObjectsV2Pages(req, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, content := range resp.Contents {
			key := *content.Key
			keysChan <- key
		}
		return true
	})
	close(keysChan)
	if err != nil {
		log.Printf("Failed to list objects for bucket %v: %v", bucket, err)
	}
	wg.Wait()
	log.Println("Done")
}

type Copier struct {
	client *s3.S3
	bucket string
	pool redis.Pool
}

func (c *Copier) Download(key string) (string, error) {
	op, err := c.client.GetObjectWithContext(context.Background(), &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}, func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", "gzip")
	})
	if err != nil {
		return "", err
	}
	defer op.Body.Close()

	bytes, err := ioutil.ReadAll(op.Body)

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (c *Copier) Upload(key, data string) error {
	conn := c.pool.Get()
	_, err := conn.Do("SET", key, data)

	if err != nil {
		log.Fatalf("Failed to write %s to redis. %v", key, err)
	}

	return err
}

func newPool(redisHost string) *redis.Pool {
	return &redis.Pool{
		MaxIdle: 80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisHost)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}

}