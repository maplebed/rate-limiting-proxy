package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	beeline "github.com/honeycombio/beeline-go"
	"github.com/honeycombio/beeline-go/wrappers/hnynethttp"
	"github.com/honeycombio/hound/leakybucket"
)

var rateOpts = leakybucket.Options{
	Capacity:      20,
	DrainAmount:   2,
	DrainPeriod:   1 * time.Second,
	CheckInterval: 1 * time.Second,
}

const downstreamTarget = "http://localhost:3000"

type app struct {
	client      *http.Client
	rateLimiter leakybucket.RateLimiter
}

func main() {

	// Initialize beeline. The only required field is WriteKey.
	beeline.Init(beeline.Config{
		WriteKey: "abcabc123123",
		Dataset:  "rate-limiting-proxy",
		// for demonstration, send the event to STDOUT instead of Honeycomb.
		// Remove the STDOUT setting when filling in a real write key.
		STDOUT: true,
	})

	a := &app{
		client: http.DefaultClient,
		// TODO replace with an in-memory rate limiter
		rateLimiter: &RandomRateLimiter{2},
	}

	http.HandleFunc("/", hnynethttp.WrapHandlerFunc(a.proxy))
	log.Fatal(http.ListenAndServe(":8080", nil))

	fmt.Printf("hello world\n")
}

func (a *app) proxy(w http.ResponseWriter, req *http.Request) {
	// check rate limits
	var rateKey string
	forwarded := req.Header.Get("X-Forwarded-For")
	beeline.AddField(req.Context(), "forwarded_for_incoming", forwarded)
	if forwarded == "" {
		rateKey = strings.Split(req.RemoteAddr, ":")[0]
	} else {
		rateKey = forwarded
	}
	beeline.AddField(req.Context(), "rate_limit_key", rateKey)
	hitCapacity := a.rateLimiter.Add(rateKey, rateOpts)
	if hitCapacity != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"rate limit exceeded; please wait 1sec and try again"}`)
		beeline.AddField(req.Context(), "error", "rate limit exceeded")
		return
	}
	// ok we're allowed to proceed, let's copy the request over to a new one and
	// dispatch it downstream
	defer req.Body.Close()
	downstreamReq, err := http.NewRequest(req.Method, downstreamTarget+req.URL.String(), req.Body)
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
