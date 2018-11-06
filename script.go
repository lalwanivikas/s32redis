package main

import (
	"context"
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

func main() {
	flag.Parse()
	if len(*bucket) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	sess, err := session.NewSession(aws.NewConfig())

	if err != nil {
		log.Fatalf("Failed to create a new session. %v", err)
	}
	s3Client := s3.New(sess, aws.NewConfig().WithRegion("eu-central-1"))

	DownloadBucket(s3Client, *bucket, *concurrency, *queueSize)
}

func DownloadBucket(client *s3.S3, bucket string, concurrency, queueSize int) {
	keysChan := make(chan string, queueSize)
	cpyr := &Copier{
		client: client,
		bucket: bucket,
	}
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysChan {
				_, err := cpyr.Fetch(key)
				if err != nil {
					log.Printf("Failed to download key %v, due to %v", key, err)
				}
				//log.Printf("%v", key)
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
}

func (c *Copier) Fetch(key string) (string, error) {
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
