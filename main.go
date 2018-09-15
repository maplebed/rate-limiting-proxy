package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/honeycombio/leakybucket"
)

// rate limiting proxy is a forwarding proxy that has a built in rate limit. In
// the style of a tarpit, it waits a random amount of time before returning when
// rate limiting connections. The forwarding target and rate limits are hard
// coded.  Based on the client IP address, it allows through bursts of up to 50
// and sustained requests of 8 per second for all GET requests. It allows a
// burst of 10 and sustained 1/sec for all other HTTP methods (eg POST, PUT,
// etc.).

const (
	// downstreamTarget is who this proxy fronts
	downstreamTarget = "http://localhost:8090"

	// the default limits for GET and all other requests
	getBurstLimit        = 50
	getThroughputLimit   = 8
	otherBurstLimit      = 10
	otherThroughputLimit = 1

	// how long should we hold on to rate limited request connections (in ms)?
	waitBaseTime = 100.0
	waitRange    = 500.0
	waitStdDev   = 100.0
)

type app struct {
	client      *http.Client
	rateLimiter map[string]*leakybucket.Bucket
	sync.Mutex
}

func main() {

	client := http.DefaultClient
	a := &app{
		client:      client,
		rateLimiter: make(map[string]*leakybucket.Bucket),
	}

	http.HandleFunc("/", a.proxy)
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}

func (a *app) proxy(w http.ResponseWriter, req *http.Request) {
	var rateKey string
	forwarded := req.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		rateKey = strings.Split(req.RemoteAddr, ":")[0]
	} else {
		rateKey = forwarded
	}

	// check rate limits
	hitCapacity := a.shouldRateLimit(req.Method, rateKey)
	if hitCapacity != nil {
		sleepTime := math.Abs(waitBaseTime + (rand.NormFloat64()*waitStdDev + waitRange))
		// sleep a random amount in the range (waitBaseTime to waitBaseTime+waitRange)
		// aka 100-600ms
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)

		// ok, go ahead and reply
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"rate limit exceeded; please wait 1sec and try again"}`)
		return
	}
	// ok we're allowed to proceed, let's copy the request over to a new one and
	// dispatch it downstream
	defer req.Body.Close()
	reqBod, _ := ioutil.ReadAll(req.Body)
	buf := bytes.NewBuffer(reqBod)
	downstreamReq, err := http.NewRequest(req.Method, downstreamTarget+req.URL.String(), buf)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"failed to create downstream request"}`)
		return
	}
	// add context to propagate the beeline trace
	downstreamReq = downstreamReq.WithContext(req.Context())
	// copy over headers from upstream to the downstream service
	for header, vals := range req.Header {
		downstreamReq.Header.Set(header, strings.Join(vals, ","))
	}
	if forwarded != "" {
		downstreamReq.Header.Set("X-Forwarded-For", forwarded+", "+req.RemoteAddr)
	} else {
		downstreamReq.Header.Set("X-Forwarded-For", req.RemoteAddr)
	}
	// call the downstream service
	resp, err := a.client.Do(downstreamReq)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"downstream target unavailable"}`)
		return
	}
	// ok, we got a response, let's pass it along
	defer resp.Body.Close()
	// copy over headers
	for header, vals := range resp.Header {
		w.Header().Set(header, strings.Join(vals, ","))
	}
	// copy over status code
	w.WriteHeader(resp.StatusCode)
	// copy over body
	io.Copy(w, resp.Body)
}

func (a *app) shouldRateLimit(method, key string) error {
	a.Lock()
	defer a.Unlock()

	var b *leakybucket.Bucket
	b, ok := a.rateLimiter[method+key]
	if !ok {
		if method == "GET" {
			b = &leakybucket.Bucket{
				Capacity:    getBurstLimit,
				DrainAmount: getThroughputLimit,
				DrainPeriod: 1 * time.Second,
			}
		} else {
			b = &leakybucket.Bucket{
				Capacity:    otherBurstLimit,
				DrainAmount: otherThroughputLimit,
				DrainPeriod: 1 * time.Second,
			}
		}
		a.rateLimiter[method+key] = b
	}
	return b.Add()
}
