package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// A read-only S3 client: just enough SigV4 to list a bucket, so the usage scan
// needs no AWS SDK (this module has no dependencies) and no extra image. It only
// ever runs inside the usage Job, which is the one place the bucket credential
// is mounted.

type awsCreds struct {
	AccessKey, SecretKey, SessionToken string
}

// loadAWSCreds reads one profile from an AWS shared-credentials file, the
// format Velero's BSL credential Secret uses.
func loadAWSCreds(path, profile string) (awsCreds, error) {
	f, err := os.Open(path)
	if err != nil {
		return awsCreds{}, err
	}
	defer f.Close()
	return parseAWSCreds(f, profile)
}

func parseAWSCreds(r io.Reader, profile string) (awsCreds, error) {
	var c awsCreds
	section := ""
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		if section != profile {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "aws_access_key_id":
			c.AccessKey = strings.TrimSpace(v)
		case "aws_secret_access_key":
			c.SecretKey = strings.TrimSpace(v)
		case "aws_session_token":
			c.SessionToken = strings.TrimSpace(v)
		}
	}
	if err := sc.Err(); err != nil {
		return c, err
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return c, fmt.Errorf("no access key in profile %q of the credentials file", profile)
	}
	return c, nil
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// awsEscape is SigV4's URI encoding: everything but unreserved characters.
func awsEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func hmacSHA256(key []byte, s string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return h.Sum(nil)
}

// signV4 signs a body-less request (S3 service) in place. Every header already
// on the request is signed, plus host.
func signV4(req *http.Request, c awsCreds, region string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	day := amzDate[:8]
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", emptySHA256)
	if c.SessionToken != "" {
		req.Header.Set("x-amz-security-token", c.SessionToken)
	}

	headers := map[string]string{"host": req.URL.Host}
	for k, v := range req.Header {
		headers[strings.ToLower(k)] = strings.TrimSpace(strings.Join(v, ","))
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signed := strings.Join(names, ";")

	q := req.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var pairs []string
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			pairs = append(pairs, awsEscape(k)+"="+awsEscape(v))
		}
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{req.Method, path, strings.Join(pairs, "&"), canonHeaders.String(), signed, emptySHA256}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/" + region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])

	key := hmacSHA256([]byte("AWS4"+c.SecretKey), day)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", c.AccessKey, scope, signed, sig))
}

type s3Client struct {
	endpoint string // bucket base URL, no trailing slash
	region   string
	creds    awsCreds
	http     *http.Client
}

// newS3Client addresses the bucket virtual-hosted style on AWS, or path style
// under a custom endpoint (the BSL's s3Url, e.g. MinIO).
func newS3Client(bucket, region, s3URL string, c awsCreds) *s3Client {
	ep := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bucket, region)
	if s3URL != "" {
		ep = strings.TrimRight(s3URL, "/") + "/" + bucket
	}
	return &s3Client{endpoint: ep, region: region, creds: c, http: &http.Client{Timeout: 60 * time.Second}}
}

type s3Error struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (s *s3Client) get(ctx context.Context, q url.Values, out any) error {
	u := s.endpoint + "/"
	if len(q) > 0 {
		// url.Values.Encode writes "uploads=" for a valueless key, which S3 accepts.
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	signV4(req, s.creds, s.region, time.Now())
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var e s3Error
		if xml.Unmarshal(body, &e) == nil && e.Code != "" {
			return fmt.Errorf("%s: %s", e.Code, e.Message)
		}
		return fmt.Errorf("S3 answered %s", resp.Status)
	}
	return xml.Unmarshal(body, out)
}

type s3Object struct {
	Key          string    `xml:"Key"`
	Size         int64     `xml:"Size"`
	LastModified time.Time `xml:"LastModified"`
}

// listObjects walks every object under prefix, calling fn for each.
func (s *s3Client) listObjects(ctx context.Context, prefix string, fn func(s3Object)) error {
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {"1000"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		var page struct {
			Contents  []s3Object `xml:"Contents"`
			Truncated bool       `xml:"IsTruncated"`
			Next      string     `xml:"NextContinuationToken"`
		}
		if err := s.get(ctx, q, &page); err != nil {
			return err
		}
		for _, o := range page.Contents {
			fn(o)
		}
		if !page.Truncated || page.Next == "" {
			return nil
		}
		token = page.Next
	}
}

type s3Upload struct {
	Key       string    `xml:"Key"`
	UploadID  string    `xml:"UploadId"`
	Initiated time.Time `xml:"Initiated"`
}

// listUploads returns unfinished multipart uploads under prefix. Their parts
// are billed but invisible to a normal listing.
func (s *s3Client) listUploads(ctx context.Context, prefix string) ([]s3Upload, error) {
	var out []s3Upload
	keyMarker, idMarker := "", ""
	for {
		q := url.Values{"uploads": {""}, "prefix": {prefix}}
		if keyMarker != "" {
			q.Set("key-marker", keyMarker)
			q.Set("upload-id-marker", idMarker)
		}
		var page struct {
			Uploads   []s3Upload `xml:"Upload"`
			Truncated bool       `xml:"IsTruncated"`
			NextKey   string     `xml:"NextKeyMarker"`
			NextID    string     `xml:"NextUploadIdMarker"`
		}
		if err := s.get(ctx, q, &page); err != nil {
			return out, err
		}
		out = append(out, page.Uploads...)
		if !page.Truncated || page.NextKey == "" {
			return out, nil
		}
		keyMarker, idMarker = page.NextKey, page.NextID
	}
}
