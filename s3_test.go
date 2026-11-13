package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// Vectors from the AWS S3 SigV4 documentation ("Examples: Signature
// Calculations in AWS Signature Version 4").
var docCreds = awsCreds{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
var docTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func sigOf(h string) string {
	_, s, _ := strings.Cut(h, "Signature=")
	return s
}

func TestSignV4GetObject(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	signV4(req, docCreds, "us-east-1", docTime)
	if got, want := sigOf(req.Header.Get("Authorization")), "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"; got != want {
		t.Errorf("signature %s, want %s", got, want)
	}
	if !strings.Contains(req.Header.Get("Authorization"), "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,") {
		t.Errorf("signed headers: %s", req.Header.Get("Authorization"))
	}
}

func TestSignV4ListObjects(t *testing.T) {
	req, _ := http.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil)
	signV4(req, docCreds, "us-east-1", docTime)
	if got, want := sigOf(req.Header.Get("Authorization")), "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"; got != want {
		t.Errorf("signature %s, want %s", got, want)
	}
}

func TestParseAWSCreds(t *testing.T) {
	f := "[other]\naws_access_key_id=NOPE\naws_secret_access_key=nope\n\n[default]\naws_access_key_id = AK\naws_secret_access_key = SK\n"
	c, err := parseAWSCreds(strings.NewReader(f), "default")
	if err != nil || c.AccessKey != "AK" || c.SecretKey != "SK" {
		t.Errorf("got %+v, %v", c, err)
	}
	if _, err := parseAWSCreds(strings.NewReader(f), "missing"); err == nil {
		t.Error("expected an error for a missing profile")
	}
}
