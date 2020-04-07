package connstr

import (
	"testing"
)

func parseOrDie(t *testing.T, connStr string) ConnSpec {
	cs, err := Parse(connStr)
	if err != nil {
		t.Fatalf("Failed to parse %s: %v", connStr, err)
	}
	return cs
}

func resolveOrDie(t *testing.T, connSpec ConnSpec) ResolvedConnSpec {
	rcs, err := Resolve(connSpec)
	if err != nil {
		t.Fatalf("Failed to resolve %s: %v", connSpec, err)
	}
	return rcs
}

func checkSpec(t *testing.T, connStr string, expectedSpec ConnSpec,
	expectMemdHosts []Address, expectHttpHosts []Address, useSsl bool,
	checkHosts bool, checkStr bool) {
	cs := parseOrDie(t, connStr)

	if checkStr && cs.String() != connStr {
		t.Fatalf("ConnStr round-trip should match. %s != %s", cs.String(), connStr)
	}

	if cs.Scheme != expectedSpec.Scheme {
		t.Fatalf("Parsed incorrect scheme")
	}

	if len(cs.Addresses) != len(expectedSpec.Addresses) {
		t.Fatalf("Some addresses were not parsed")
	}

	for i, csAddr := range cs.Addresses {
		expectedAddr := expectedSpec.Addresses[i]

		if csAddr.Host != expectedAddr.Host {
			t.Fatalf("Parsed incorrect host. %s != %s", csAddr.Host, expectedAddr.Host)
		}

		if csAddr.Port != expectedAddr.Port {
			t.Fatalf("Parsed incorrect port. %d != %d", csAddr.Port, expectedAddr.Port)
		}
	}

	if cs.Bucket != expectedSpec.Bucket {
		t.Fatalf("Parsed incorrect bucket. %s != %s", cs.Bucket, expectedSpec.Bucket)
	}

	if len(cs.Options) != len(expectedSpec.Options) {
		t.Fatalf("Some options were not parsed")
	}

	for key, opts := range cs.Options {
		expectedOpts := expectedSpec.Options[key]

		if len(opts) != len(expectedOpts) {
			t.Fatalf("Some option values were not parsed")
		}

		for i, opt := range opts {
			expectedOpt := expectedOpts[i]

			if opt != expectedOpt {
				t.Fatalf("Parsed incorrect option value. %s != %s", opt, expectedOpt)
			}
		}
	}

	rcs := resolveOrDie(t, cs)

	if rcs.UseSsl != useSsl {
		t.Fatalf("Did not correctly mark SSL")
	}

	if checkHosts {
		if len(rcs.MemdHosts) != len(expectMemdHosts) {
			t.Fatalf("Some memd hosts were missing")
		}

		for i, host := range rcs.MemdHosts {
			expectHost := expectMemdHosts[i]

			if host.Host != expectHost.Host {
				t.Fatalf("Resolved incorrect memd host. %s != %s", host.Host, expectHost.Host)
			}

			if host.Port != expectHost.Port {
				t.Fatalf("Resolved incorrect memd port. %d != %d", host.Port, expectHost.Port)
			}
		}

		if len(rcs.MemdHosts) != len(expectMemdHosts) {
			t.Fatalf("Some memd hosts were missing")
		}

		for i, host := range rcs.HttpHosts {
			expectHost := expectHttpHosts[i]

			if host.Host != expectHost.Host {
				t.Fatalf("Resolved incorrect http host. %s != %s", host.Host, expectHost.Host)
			}

			if host.Port != expectHost.Port {
				t.Fatalf("Resolved incorrect http port. %d != %d", host.Port, expectHost.Port)
			}
		}
	}
}

func TestParseBasic(t *testing.T) {
	checkSpec(t, "couchbase://1.2.3.4", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"1.2.3.4", -1}},
	}, []Address{
		{"1.2.3.4", DefaultMemdPort},
	}, []Address{
		{"1.2.3.4", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "couchbase://[2001:4860:4860::8888]", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"[2001:4860:4860::8888]", -1}},
	}, []Address{
		{"[2001:4860:4860::8888]", DefaultMemdPort},
	}, []Address{
		{"[2001:4860:4860::8888]", DefaultHttpPort},
	}, false, true, true)

	_, err := Parse("blah://foo.com")
	if err == nil {
		t.Fatalf("Expected error for bad scheme")
	}

	checkSpec(t, "couchbase://", ConnSpec{
		Scheme: "couchbase",
	}, []Address{
		{"127.0.0.1", DefaultMemdPort},
	}, []Address{
		{"127.0.0.1", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "couchbase://?", ConnSpec{
		Scheme: "couchbase",
	}, []Address{
		{"127.0.0.1", DefaultMemdPort},
	}, []Address{
		{"127.0.0.1", DefaultHttpPort},
	}, false, true, false)

	checkSpec(t, "1.2.3.4", ConnSpec{
		Addresses: []Address{
			{"1.2.3.4", -1},
		},
	}, []Address{
		{"1.2.3.4", DefaultMemdPort},
	}, []Address{
		{"1.2.3.4", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "[2001:4860:4860::8888]", ConnSpec{
		Addresses: []Address{
			{"[2001:4860:4860::8888]", -1}},
	}, []Address{
		{"[2001:4860:4860::8888]", DefaultMemdPort},
	}, []Address{
		{"[2001:4860:4860::8888]", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "1.2.3.4:8091", ConnSpec{
		Addresses: []Address{
			{"1.2.3.4", 8091},
		},
	}, []Address{
		{"1.2.3.4", DefaultMemdPort},
	}, []Address{
		{"1.2.3.4", DefaultHttpPort},
	}, false, true, true)

	cs := parseOrDie(t, "1.2.3.4:999")
	_, err = Resolve(cs)
	if err == nil {
		t.Fatalf("Expected error with non-default port without scheme")
	}
}

func TestParseHosts(t *testing.T) {
	checkSpec(t, "couchbase://foo.com,bar.com,baz.com", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", -1},
			{"bar.com", -1},
			{"baz.com", -1},
		},
	}, []Address{
		{"foo.com", DefaultMemdPort},
		{"bar.com", DefaultMemdPort},
		{"baz.com", DefaultMemdPort},
	}, []Address{
		{"foo.com", DefaultHttpPort},
		{"bar.com", DefaultHttpPort},
		{"baz.com", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "couchbase://[2001:4860:4860::8822],[2001:4860:4860::8833]:888", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"[2001:4860:4860::8822]", -1},
			{"[2001:4860:4860::8833]", 888},
		},
	}, []Address{
		{"[2001:4860:4860::8822]", DefaultMemdPort},
		{"[2001:4860:4860::8833]", 888},
	}, []Address{
		{"[2001:4860:4860::8822]", DefaultHttpPort},
	}, false, true, true)

	// Parse using legacy format
	cs := parseOrDie(t, "couchbase://foo.com:8091")
	_, err := Resolve(cs)
	if err == nil {
		t.Fatalf("Expected error for couchbase://XXX:8091")
	}

	checkSpec(t, "couchbase://foo.com:4444", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", 4444},
		},
	}, []Address{
		{"foo.com", 4444},
	}, nil, false, true, true)

	checkSpec(t, "couchbases://foo.com:4444", ConnSpec{
		Scheme: "couchbases",
		Addresses: []Address{
			{"foo.com", 4444},
		},
	}, []Address{
		{"foo.com", 4444},
	}, []Address{
		{"foo.com", 4444},
	}, true, true, true)

	checkSpec(t, "couchbases://", ConnSpec{
		Scheme: "couchbases",
	}, []Address{
		{"127.0.0.1", DefaultSslMemdPort},
	}, []Address{
		{"127.0.0.1", DefaultSslHttpPort},
	}, true, true, true)

	checkSpec(t, "couchbase://foo.com,bar.com:4444", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", -1},
			{"bar.com", 4444},
		},
	}, []Address{
		{"foo.com", DefaultMemdPort},
		{"bar.com", 4444},
	}, []Address{
		{"foo.com", DefaultHttpPort},
	}, false, true, true)

	checkSpec(t, "couchbase://foo.com;bar.com;baz.com", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", -1},
			{"bar.com", -1},
			{"baz.com", -1},
		},
	}, []Address{
		{"foo.com", DefaultMemdPort},
		{"bar.com", DefaultMemdPort},
		{"baz.com", DefaultMemdPort},
	}, []Address{
		{"foo.com", DefaultHttpPort},
		{"bar.com", DefaultHttpPort},
		{"baz.com", DefaultHttpPort},
	}, false, true, false)
}

func TestParseBucket(t *testing.T) {
	checkSpec(t, "couchbase://foo.com/user", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", -1},
		},
		Bucket: "user",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase://foo.com/user/", ConnSpec{
		Scheme: "couchbase",
		Addresses: []Address{
			{"foo.com", -1},
		},
		Bucket: "user/",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase:///default", ConnSpec{
		Scheme: "couchbase",
		Bucket: "default",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase:///default", ConnSpec{
		Scheme: "couchbase",
		Bucket: "default",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase:///default", ConnSpec{
		Scheme: "couchbase",
		Bucket: "default",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase:///default?", ConnSpec{
		Scheme: "couchbase",
		Bucket: "default",
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase:///%2FUsers%2F?", ConnSpec{
		Scheme: "couchbase",
		Bucket: "/Users/",
	}, nil, nil, false, false, false)
}

func TestOptionsPassthrough(t *testing.T) {
	checkSpec(t, "couchbase:///?foo=bar", ConnSpec{
		Scheme: "couchbase",
		Options: map[string][]string{
			"foo": {"bar"},
		},
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase://?foo=bar", ConnSpec{
		Scheme: "couchbase",
		Options: map[string][]string{
			"foo": {"bar"},
		},
	}, nil, nil, false, false, true)

	checkSpec(t, "couchbase://?foo=fooval&bar=barval", ConnSpec{
		Scheme: "couchbase",
		Options: map[string][]string{
			"foo": {"fooval"},
			"bar": {"barval"},
		},
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase://?foo=fooval&bar=barval&", ConnSpec{
		Scheme: "couchbase",
		Options: map[string][]string{
			"foo": {"fooval"},
			"bar": {"barval"},
		},
	}, nil, nil, false, false, false)

	checkSpec(t, "couchbase://?foo=val1&foo=val2&", ConnSpec{
		Scheme: "couchbase",
		Options: map[string][]string{
			"foo": {"val1", "val2"},
		},
	}, nil, nil, false, false, false)
}
