package redis

import (
    "log"
    "time"

    "github.com/coredns/coredns/plugin"
    "github.com/coredns/coredns/plugin/pkg/dnsutil"
    "github.com/coredns/coredns/request"
    "github.com/miekg/dns"
    "golang.org/x/net/context"
)

// ServeDNS implements the plugin.Handler interface.
func (redis *Redis) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
    log.Println("serveDNS")
    state := request.Request{W: w, Req: r}

    qname := state.Name()
    qtype := state.Type()

    answers := make([]dns.RR, 0, 10)
    extras := make([]dns.RR, 0, 10)
    ns := make([]dns.RR, 0, 10)
    m := new(dns.Msg)

    log.Println("name : ", qname)
    log.Println("type : ", qtype)

    if time.Since(redis.LastZoneUpdate) > ZoneUpdateTime {
        redis.LoadZones()
    }

    zone := plugin.Zones(redis.Zones).Matches(qname)
    log.Println("zone : ", zone)
    if zone == "" {
        return plugin.NextOrFailure(qname, redis.Next, ctx, w, r)
    }

    z := redis.load(zone)
    if z == nil {
        m.SetRcode(state.Req, dns.RcodeServerFailure)
        return redis.sendResponse(state, w, m, r)
    }

    location := redis.findLocation(qname, z)
    if len(location) == 0 { // empty, no results
        record := redis.get(z.Name, z)
        ns = redis.AuthoritativeResponse(qname, z, record)
        m.Ns = append(m.Ns, ns...)
        m.SetRcode(state.Req, dns.RcodeNameError)
        return redis.sendResponse(state, w, m, r)
    }
    log.Println("location : ", location)

    record := redis.get(location, z)

    switch qtype {
    case "A":
        answers, extras = redis.A(qname, z, record)
    case "AAAA":
        answers, extras = redis.AAAA(qname, z, record)
    case "CNAME":
        answers, extras = redis.CNAME(qname, z, record)
    case "TXT":
        answers, extras = redis.TXT(qname, z, record)
    case "NS":
        answers, extras = redis.NS(qname, z, record)
    case "MX":
        answers, extras = redis.MX(qname, z, record)
    case "SRV":
        answers, extras = redis.SRV(qname, z, record)
    case "SOA":
        answers, extras = redis.SOA(qname, z, record)
    case "HINFO":
    case "ANY":
        answers, extras = redis.ANY(qname, z, record)
    case "IXFR":
    case "AXFR":
        answers, extras = redis.XFR(qname, z, record)
    default:
        //redis.errorResponse(state, zone, dns.RcodeNotImplemented, nil)
        m.SetRcode(state.Req, dns.RcodeNotImplemented)
    }

    if record.HasSOA() && len(answers) <= 0 {
        ns = redis.AuthoritativeResponse(qname, z, record)
    }

    m.Answer = append(m.Answer, answers...)
    m.Extra = append(m.Extra, extras...)
    m.Ns = append(m.Ns, ns...)

    return redis.sendResponse(state, w, m, r)
}

// Name implements the Handler interface.
func (redis *Redis) Name() string { return "redis" }

func (redis *Redis) sendResponse(state request.Request, w dns.ResponseWriter, m *dns.Msg, r *dns.Msg) (int, error) {
    m.SetReply(r)
    m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

    m = dnsutil.Dedup(m)
    state.SizeAndDo(m)
    m, _ = state.Scrub(m)
    w.WriteMsg(m)
    return dns.RcodeSuccess, nil
}
