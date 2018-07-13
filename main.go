package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	beeline "github.com/honeycombio/beeline-go"
	"github.com/honeycombio/beeline-go/wrappers/hnynethttp"
	"github.com/honeycombio/leakybucket"
)

const downstreamTarget = "http://localhost:3000"

type app struct {
	client      *http.Client
	rateLimiter map[string]*leakybucket.Bucket
	sync.Mutex
}

func main() {

	wk := os.Getenv("HONEYCOMB_WRITEKEY")
	var useStdout bool
	if wk == "" {
		useStdout = true
	}
	// Initialize beeline. The only required field is WriteKey.
	beeline.Init(beeline.Config{
		WriteKey: wk,
		Dataset:  "rate-limiting-proxy",
		// In no writekey is configured, send the event to STDOUT instead of Honeycomb.
		STDOUT: useStdout,
	})

	a := &app{
		client:      http.DefaultClient,
		rateLimiter: make(map[string]*leakybucket.Bucket),
	}

	http.HandleFunc("/", hnynethttp.WrapHandlerFunc(a.proxy))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (a *app) proxy(w http.ResponseWriter, req *http.Request) {
	var rateKey string
	forwarded := req.Header.Get("X-Forwarded-For")
	beeline.AddField(req.Context(), "forwarded_for_incoming", forwarded)
	if forwarded == "" {
		rateKey = strings.Split(req.RemoteAddr, ":")[0]
	} else {
		rateKey = forwarded
	}

	// check rate limits
	beeline.AddField(req.Context(), "rate_limit_key", rateKey)
	hitCapacity := a.shouldRateLimit(req.Method, rateKey)
	if hitCapacity != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"rate limit exceeded; please wait 1sec and try again"}`)
		beeline.AddField(req.Context(), "error", "rate limit exceeded")
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
		beeline.AddField(req.Context(), "error", err)
		beeline.AddField(req.Context(), "error_detail", "failed to create downstream request")
		return
	}
	// copy over headers from upstream to the downstream service
	for header, vals := range req.Header {
		downstreamReq.Header.Set(header, strings.Join(vals, ","))
	}
	if forwarded != "" {
		downstreamReq.Header.Set("X-Forwarded-For", forwarded+", "+req.RemoteAddr)
	} else {
		downstreamReq.Header.Set("X-Forwarded-For", req.RemoteAddr)
	}
	beeline.AddField(req.Context(), "forwarded_for_outgoing", downstreamReq.Header.Get("X-Forwarded-For"))
	// call the downstream service
	resp, err := a.client.Do(downstreamReq)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"downstream target unavailable"}`)
		beeline.AddField(req.Context(), "error", err)
		beeline.AddField(req.Context(), "error_detail", "downstream target unavailable")
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
				Capacity:    50,
				DrainAmount: 8,
				DrainPeriod: 1 * time.Second,
			}
		} else {
			b = &leakybucket.Bucket{
				Capacity:    10,
				DrainAmount: 1,
				DrainPeriod: 1 * time.Second,
			}
		}
		a.rateLimiter[method+key] = b
	}
	return b.Add()
}
