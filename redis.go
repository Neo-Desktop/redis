package redis

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"github.com/coredns/coredns/plugin"

	redisCon "github.com/garyburd/redigo/redis"
	"strconv"
)

type Redis struct {
	Next           plugin.Handler
	Pool           *redisCon.Pool
	Ttl            uint32
	Zones          []string
	LastZoneUpdate time.Time
	redisAddress   string
	redisPassword  string
	redisDbIndex   int
	connectTimeout int
	readTimeout    int
	keyPrefix      string
	keySuffix      string
}

type Zone struct {
	Name      string
	Locations map[string]struct{}
}

type Record struct {
	A     []ARecord     `json:"a,omitempty"`
	AAAA  []AAAARecord  `json:"aaaa,omitempty"`
	TXT   []TXTRecord   `json:"txt,omitempty"`
	CNAME []CNAMERecord `json:"cname,omitempty"`
	NS    []NSRecord    `json:"ns,omitempty"`
	MX    []MXRecord    `json:"mx,omitempty"`
	SRV   []SRVRecord   `json:"srv,omitempty"`
	SOA   *SOARecord    `json:"soa,omitempty"`
}

func (record *Record) HasSOA() bool {
	return (SOARecord{}) == *record.SOA
}

func (record *Record) HasA() bool {
	return len(record.A) > 0
}

func (record *Record) HasAAAA() bool {
	return len(record.SRV) > 0
}

func (record *Record) HasCNAME() bool {
	return len(record.SRV) > 0
}

func (record *Record) HasNS() bool {
	return len(record.SRV) > 0
}

func (record *Record) HasNX() bool {
	return len(record.SRV) > 0
}

func (record *Record) HasSRV() bool {
	return len(record.SRV) > 0
}

type ARecord struct {
	Ttl uint32 `json:"ttl,omitempty"`
	Ip  net.IP `json:"ip"`
}

type AAAARecord struct {
	Ttl uint32 `json:"ttl,omitempty"`
	Ip  net.IP `json:"ip"`
}

type TXTRecord struct {
	Ttl  uint32 `json:"ttl,omitempty"`
	Text string `json:"text"`
}

type CNAMERecord struct {
	Ttl  uint32 `json:"ttl,omitempty"`
	Host string `json:"host"`
}

type NSRecord struct {
	Ttl  uint32 `json:"ttl,omitempty"`
	Host string `json:"host"`
}

type MXRecord struct {
	Ttl        uint32 `json:"ttl,omitempty"`
	Host       string `json:"host"`
	Preference uint16 `json:"preference"`
}

type SRVRecord struct {
	Ttl      uint32 `json:"ttl,omitempty"`
	Priority uint16 `json:"priority"`
	Weight   uint16 `json:"weight"`
	Port     uint16 `json:"port"`
	Target   string `json:"target"`
}

type SOARecord struct {
	Ttl     uint32 `json:"ttl,omitempty"`
	Ns      string `json:"ns"`
	MBox    string `json:"MBox"`
	Serial  uint32 `json:"serial"`
	Refresh uint32 `json:"refresh"`
	Retry   uint32 `json:"retry"`
	Expire  uint32 `json:"expire"`
	MinTtl  uint32 `json:"minttl"`
}

func (redis *Redis) LoadZones() {
	var (
		reply interface{}
		err   error
		zones []string
	)

	conn := redis.Pool.Get()
	if conn == nil {
		fmt.Println("error connecting to redis")
		return
	}
	defer conn.Close()

	reply, err = conn.Do("KEYS", redis.keyPrefix+"*"+redis.keySuffix)
	if err != nil {
		return
	}
	zones, err = redisCon.Strings(reply, nil)
	for i, _ := range zones {
		zones[i] = strings.TrimPrefix(zones[i], redis.keyPrefix)
		zones[i] = strings.TrimSuffix(zones[i], redis.keySuffix)
	}
	redis.LastZoneUpdate = time.Now()
	redis.Zones = zones
}

func (redis *Redis) A(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, a := range record.A {
		if a.Ip == nil {
			continue
		}
		r := new(dns.A)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeA,
			Class: dns.ClassINET, Ttl: redis.minTtl(a.Ttl)}
		r.A = a.Ip
		answers = append(answers, r)
	}
	return
}

func (redis Redis) AAAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, aaaa := range record.AAAA {
		if aaaa.Ip == nil {
			continue
		}
		r := new(dns.AAAA)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA,
			Class: dns.ClassINET, Ttl: redis.minTtl(aaaa.Ttl)}
		r.AAAA = aaaa.Ip
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) CNAME(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, cname := range record.CNAME {
		if len(cname.Host) == 0 {
			continue
		}
		r := new(dns.CNAME)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME,
			Class: dns.ClassINET, Ttl: redis.minTtl(cname.Ttl)}
		r.Target = dns.Fqdn(cname.Host)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) TXT(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, txt := range record.TXT {
		if len(txt.Text) == 0 {
			continue
		}
		r := new(dns.TXT)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeTXT,
			Class: dns.ClassINET, Ttl: redis.minTtl(txt.Ttl)}
		r.Txt = split255(txt.Text)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) NS(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, ns := range record.NS {
		if len(ns.Host) == 0 {
			continue
		}
		r := new(dns.NS)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeNS,
			Class: dns.ClassINET, Ttl: redis.minTtl(ns.Ttl)}
		r.Ns = ns.Host
		answers = append(answers, r)
		extras = append(extras, redis.hosts(ns.Host, z)...)
	}
	return
}

func (redis *Redis) MX(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, mx := range record.MX {
		if len(mx.Host) == 0 {
			continue
		}
		r := new(dns.MX)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeMX,
			Class: dns.ClassINET, Ttl: redis.minTtl(mx.Ttl)}
		r.Mx = mx.Host
		r.Preference = mx.Preference
		answers = append(answers, r)
		extras = append(extras, redis.hosts(mx.Host, z)...)
	}
	return
}

func (redis *Redis) SRV(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	for _, srv := range record.SRV {
		if len(srv.Target) == 0 {
			continue
		}
		r := new(dns.SRV)
		r.Hdr = dns.RR_Header{Name: name, Rrtype: dns.TypeSRV,
			Class: dns.ClassINET, Ttl: redis.minTtl(srv.Ttl)}
		r.Target = srv.Target
		r.Weight = srv.Weight
		r.Port = srv.Port
		r.Priority = srv.Priority
		answers = append(answers, r)
		extras = append(extras, redis.hosts(srv.Target, z)...)
	}
	return
}

func (redis *Redis) ANY(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	log.Println("in redis.any()")
	answersOut := make([]dns.RR, 0, 10)
	extrasOut := make([]dns.RR, 0, 10)

	answers, extras = redis.NS(name, z, record)
	log.Printf("NS returned: answers: %d extras %d\n", len(answers), len(extras))
	answersOut = append(answersOut, answers...)
	extrasOut = append(extrasOut, extras...)

	answers, extras = redis.A(name, z, record)
	log.Printf("A returned: answers: %d extras %d\n", len(answers), len(extras))
	answersOut = append(answersOut, answers...)
	extrasOut = append(extrasOut, extras...)

	answers, extras = redis.AAAA(name, z, record)
	log.Printf("AAAA returned: answers: %d extras %d\n", len(answers), len(extras))
	answersOut = append(answersOut, answers...)
	extrasOut = append(extrasOut, extras...)

	answers, extras = redis.CNAME(name, z, record)
	log.Printf("CNAME returned: answers: %d extras %d\n", len(answers), len(extras))
	answersOut = append(answersOut, answers...)
	extrasOut = append(extrasOut, extras...)

	log.Printf("redis.any() returning answers: %d. extras: %d\n", len(answersOut), len(extrasOut))
	return answersOut, extrasOut
}

func (redis *Redis) SOA(name string, z *Zone, record *Record, qtype string) (answers, extras []dns.RR) {
	r := new(dns.SOA)
	r.Hdr = dns.RR_Header{Name: z.Name, Rrtype: dns.TypeSOA,
		Class: dns.ClassINET, Ttl: redis.minTtl(record.SOA.Ttl)}
	r.Ns = record.SOA.Ns
	r.Mbox = record.SOA.MBox
	r.Refresh = record.SOA.Refresh
	r.Retry = record.SOA.Retry
	r.Expire = record.SOA.Expire
	r.Minttl = record.SOA.MinTtl
	r.Serial = record.SOA.Serial

	if record.SOA.Ns == "" && qtype == "SOA" {
		extras = append(extras, r)
	} else {
		answers = append(answers, r)
	}

	return
}

func (redis *Redis) AuthoritativeResponse(name string, z *Zone, record *Record) (ns []dns.RR) {
	r := new(dns.SOA)
	r.Hdr = dns.RR_Header{Name: z.Name, Rrtype: dns.TypeSOA,
		Class: dns.ClassINET, Ttl: redis.minTtl(record.SOA.Ttl)}
	r.Ns = record.SOA.Ns
	r.Mbox = record.SOA.MBox
	r.Refresh = record.SOA.Refresh
	r.Retry = record.SOA.Retry
	r.Expire = record.SOA.Expire
	r.Minttl = record.SOA.MinTtl
	r.Serial = record.SOA.Serial

	ns = append(ns, r)

	return
}

func (redis *Redis) hosts(name string, z *Zone) []dns.RR {
	var (
		record  *Record
		answers []dns.RR
	)
	location := redis.findLocation(name, z)
	if location == "" {
		return nil
	}
	record = redis.get(location, z)
	a, _ := redis.A(name, z, record)
	answers = append(answers, a...)
	aaaa, _ := redis.AAAA(name, z, record)
	answers = append(answers, aaaa...)
	cname, _ := redis.CNAME(name, z, record)
	answers = append(answers, cname...)
	return answers
}

func (redis *Redis) Serial(count uint, now time.Time) uint32 {
	out, err := strconv.ParseInt(fmt.Sprintf("%04d%02d%02d%02d", now.Year(), now.Month(), now.Day(), count), 10, 8)
	if err != nil {
		log.Fatalln(err)
	}
	return uint32(out)
}

func (redis *Redis) minTtl(ttlIn uint32) uint32 {
	if redis.Ttl == 0 && ttlIn == 0 {
		return uint32(TTL)
	}
	if redis.Ttl == 0 {
		return ttlIn
	}
	if ttlIn == 0 {
		return redis.Ttl
	}
	if redis.Ttl < ttlIn {
		return redis.Ttl
	}
	return ttlIn
}

func (redis *Redis) findLocation(query string, z *Zone) string {
	var (
		ok                                 bool
		closestEncloser, sourceOfSynthesis string
	)

	// request for zone records
	if query == z.Name {
		return query
	}

	query = strings.TrimSuffix(query, "."+z.Name)

	if _, ok = z.Locations[query]; ok {
		return query
	}

	closestEncloser, sourceOfSynthesis, ok = splitQuery(query)
	for ok {
		ceExists := keyMatches(closestEncloser, z) || keyExists(closestEncloser, z)
		ssExists := keyExists(sourceOfSynthesis, z)
		if ceExists {
			if ssExists {
				return sourceOfSynthesis
			} else {
				return ""
			}
		} else {
			closestEncloser, sourceOfSynthesis, ok = splitQuery(closestEncloser)
		}
	}
	return ""
}

func (redis *Redis) get(key string, z *Zone) *Record {
	var (
		err   error
		reply interface{}
		val   string
	)
	conn := redis.Pool.Get()
	if conn == nil {
		log.Fatalln("error connecting to redis")
		return nil
	}
	defer conn.Close()

	var label string
	if key == z.Name {
		label = "@"
	} else {
		label = key
	}

	reply, err = conn.Do("HGET", redis.keyPrefix+z.Name+redis.keySuffix, label)
	if err != nil {
		return nil
	}
	val, err = redisCon.String(reply, nil)
	if err != nil {
		return nil
	}
	r := new(Record)
	err = json.Unmarshal([]byte(val), r)
	if err != nil {
		log.Fatalln("parse error : ", val, err)
		return nil
	}
	return r
}

func keyExists(key string, z *Zone) bool {
	_, ok := z.Locations[key]
	return ok
}

func keyMatches(key string, z *Zone) bool {
	for value := range z.Locations {
		if strings.HasSuffix(value, key) {
			return true
		}
	}
	return false
}

func splitQuery(query string) (string, string, bool) {
	if query == "" {
		return "", "", false
	}
	var (
		splits            []string
		closestEncloser   string
		sourceOfSynthesis string
	)
	splits = strings.SplitAfterN(query, ".", 2)
	if len(splits) == 2 {
		closestEncloser = splits[1]
		sourceOfSynthesis = "*." + closestEncloser
	} else {
		closestEncloser = ""
		sourceOfSynthesis = "*"
	}
	return closestEncloser, sourceOfSynthesis, true
}

func (redis *Redis) connect() {
	redis.Pool = &redisCon.Pool{
		Dial: func() (redisCon.Conn, error) {
			opts := []redisCon.DialOption{}
			if redis.redisPassword != "" {
				opts = append(opts, redisCon.DialPassword(redis.redisPassword))
			}
			if redis.connectTimeout != 0 {
				opts = append(opts, redisCon.DialConnectTimeout(time.Duration(redis.connectTimeout)*time.Millisecond))
			}
			if redis.readTimeout != 0 {
				opts = append(opts, redisCon.DialReadTimeout(time.Duration(redis.readTimeout)*time.Millisecond))
			}
			if redis.redisDbIndex != 0 {
				opts = append(opts, redisCon.DialDatabase(redis.redisDbIndex))
			}

			return redisCon.Dial("tcp", redis.redisAddress, opts...)
		},
	}
}

func (redis *Redis) save(zone string, subdomain string, value string) error {
	var err error

	conn := redis.Pool.Get()
	if conn == nil {
		log.Fatalln("error connecting to redis")
		return nil
	}
	defer conn.Close()

	_, err = conn.Do("HSET", redis.keyPrefix+zone+redis.keySuffix, subdomain, value)
	return err
}

func (redis *Redis) load(zone string) *Zone {
	var (
		reply interface{}
		err   error
		vals  []string
	)

	conn := redis.Pool.Get()
	if conn == nil {
		log.Fatalln("error connecting to redis")
		return nil
	}
	defer conn.Close()

	reply, err = conn.Do("HKEYS", redis.keyPrefix+zone+redis.keySuffix)
	if err != nil {
		return nil
	}
	z := new(Zone)
	z.Name = zone
	vals, err = redisCon.Strings(reply, nil)
	if err != nil {
		return nil
	}
	z.Locations = make(map[string]struct{})
	for _, val := range vals {
		z.Locations[val] = struct{}{}
	}

	return z
}

func split255(s string) []string {
	if len(s) < 255 {
		return []string{s}
	}
	sx := []string{}
	p, i := 0, 255
	for {
		if i <= len(s) {
			sx = append(sx, s[p:i])
		} else {
			sx = append(sx, s[p:])
			break

		}
		p, i = p+255, i+255
	}

	return sx
}

var (
	TTL               = 300
	DefaultHostmaster = "hostmaster"
	ZoneUpdateTime    = 1 * time.Minute
)
