package main

import "testing"

func TestExtractConfigFlag(t *testing.T) {
	cases := []struct {
		args []string
		path string
		ok   bool
	}{
		{[]string{"--id", "n1"}, "", false},
		{[]string{"--config", "foo.yaml"}, "foo.yaml", true},
		{[]string{"--config=foo.yaml"}, "foo.yaml", true},
		{[]string{"-config=foo.yaml"}, "foo.yaml", true},
		{[]string{"--config"}, "", false}, // no path after
	}
	for _, c := range cases {
		got, ok := extractConfigFlag(c.args)
		if got != c.path || ok != c.ok {
			t.Errorf("extractConfigFlag(%v) = %q,%v; want %q,%v", c.args, got, ok, c.path, c.ok)
		}
	}
}
