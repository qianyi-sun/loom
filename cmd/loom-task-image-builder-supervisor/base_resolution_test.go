package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const baseResolutionTestRoot = "sha256:b6ec3b0cc39f2c4050beca5bc232260a4a09925ac19bfaac80e830066b5b1297"
const baseResolutionTestImage = "sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40"

func baseResolutionFixture(platform, bases string) []byte {
	return []byte(`{"containerimage.digest":"` + baseResolutionTestRoot + `","containerimage.config.digest":"sha256:bca44aac00510a017b2bdf51659857715fb6df19802e8a7e8e340f6cacbeca92","loom.task-image-base-resolution.v1":{"schema":"loom.task-image-base-resolution/v1","solve_ref":"solve_1-abc","platform":"` + platform + `","output_digest":"` + baseResolutionTestRoot + `","observed_base_digests":` + bases + `}}`)
}

func TestBaseResolutionAcceptsExplicitScratchAndImageEvidence(t *testing.T) {
	for _, platform := range []string{"linux/amd64", "linux/arm64"} {
		for _, bases := range []string{`[]`, `["` + baseResolutionTestImage + `"]`, `["` + baseResolutionTestRoot + `"]`} {
			t.Run(platform+bases, func(t *testing.T) {
				payload := baseResolutionFixture(platform, bases)
				evidence, err := parseBaseResolutionMetadata(payload, "solve_1-abc", platform, baseResolutionTestRoot)
				if err != nil {
					t.Fatal(err)
				}
				want := `{"schema":"loom.task-image-base-resolution/v1","solve_ref":"solve_1-abc","platform":"` + platform + `","output_digest":"` + baseResolutionTestRoot + `","observed_base_digests":` + bases + `}`
				if evidence.JSON() != want {
					t.Fatalf("evidence = %s, want %s", evidence.JSON(), want)
				}
				payload[0] = 'x'
				if evidence.JSON() != want {
					t.Fatal("accepted evidence retained mutable input bytes")
				}
			})
		}
	}
}

func TestBaseResolutionRejectsMissingMalformedAndSubstitutedEvidence(t *testing.T) {
	baseline := string(baseResolutionFixture("linux/amd64", `[]`))
	cases := map[string]string{
		"missing":                  `{"containerimage.digest":"` + baseResolutionTestRoot + `"}`,
		"null record":              strings.Replace(baseline, `{"schema":`, `null,"unexpected":{"schema":`, 1),
		"unknown schema":           strings.Replace(baseline, `resolution/v1`, `resolution/v2`, 1),
		"wrong solve":              strings.Replace(baseline, `solve_1-abc`, `solve_other`, 1),
		"wrong platform":           strings.Replace(baseline, `linux/amd64`, `linux/arm64`, 1),
		"wrong output":             strings.Replace(baseline, `"output_digest":"`+baseResolutionTestRoot, `"output_digest":"`+baseResolutionTestImage, 1),
		"wrong exporter output":    strings.Replace(baseline, baseResolutionTestRoot, baseResolutionTestImage, 1),
		"missing exporter output":  strings.Replace(baseline, `"containerimage.digest":"`+baseResolutionTestRoot+`",`, ``, 1),
		"null bases":               strings.Replace(baseline, `"observed_base_digests":[]`, `"observed_base_digests":null`, 1),
		"missing bases":            strings.Replace(baseline, `,"observed_base_digests":[]`, ``, 1),
		"unknown record field":     strings.Replace(baseline, `"observed_base_digests":[]`, `"observed_base_digests":[],"extra":true`, 1),
		"duplicate record field":   strings.Replace(baseline, `"solve_ref":`, `"solve_ref":"old","solve_ref":`, 1),
		"duplicate exporter field": strings.Replace(baseline, `"containerimage.digest":`, `"containerimage.digest":"old","containerimage.digest":`, 1),
		"duplicate record":         strings.Replace(baseline, `"loom.task-image-base-resolution.v1":`, `"loom.task-image-base-resolution.v1":{},"loom.task-image-base-resolution.v1":`, 1),
		"case alias":               strings.Replace(baseline, `"schema":`, `"Schema":`, 1),
		"trailing JSON":            baseline + `{}`,
		"invalid UTF8":             baseline[:len(baseline)-1] + string([]byte{0xff}) + `}`,
		"oversize envelope":        baseline[:len(baseline)-1] + `,"unrelated":"` + strings.Repeat("x", 64*1024) + `"}`,
		"oversize record":          strings.Replace(baseline, `"schema":`, strings.Repeat(" ", 16*1024)+`"schema":`, 1),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBaseResolutionMetadata([]byte(payload), "solve_1-abc", "linux/amd64", baseResolutionTestRoot); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}

func TestBaseResolutionRejectsInvalidBaseLists(t *testing.T) {
	for name, bases := range map[string]string{
		"scalar": `true`, "object": `{}`, "null item": `[null]`, "number": `[1]`,
		"zero":      `["sha256:` + strings.Repeat("0", 64) + `"]`,
		"uppercase": `["sha256:` + strings.Repeat("A", 64) + `"]`,
		"short":     `["sha256:a"]`, "algorithm": `["sha512:` + strings.Repeat("a", 64) + `"]`,
		"duplicate": `["` + baseResolutionTestImage + `","` + baseResolutionTestImage + `"]`,
		"unsorted":  `["` + baseResolutionTestImage + `","` + baseResolutionTestRoot + `"]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBaseResolutionMetadata(baseResolutionFixture("linux/amd64", bases), "solve_1-abc", "linux/amd64", baseResolutionTestRoot); err == nil {
				t.Fatal("invalid base list accepted")
			}
		})
	}
}

func TestBaseResolutionEnforcesImageCountAndExpectedBinding(t *testing.T) {
	bases := make([]string, 0, 129)
	for i := 1; i <= 129; i++ {
		bases = append(bases, fmt.Sprintf("sha256:%064x", i))
	}
	for _, n := range []int{128, 129} {
		encoded, err := json.Marshal(bases[:n])
		if err != nil {
			t.Fatal(err)
		}
		_, err = parseBaseResolutionMetadata(baseResolutionFixture("linux/amd64", string(encoded)), "solve_1-abc", "linux/amd64", baseResolutionTestRoot)
		if (err == nil) != (n == 128) {
			t.Fatalf("count %d: %v", n, err)
		}
	}
	for _, ref := range []string{"", "../escape", "-prefix", "solve\n", strings.Repeat("a", 129)} {
		encoded, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}
		payload := strings.ReplaceAll(string(baseResolutionFixture("linux/amd64", `[]`)), `"solve_1-abc"`, string(encoded))
		if _, err := parseBaseResolutionMetadata([]byte(payload), ref, "linux/amd64", baseResolutionTestRoot); err == nil {
			t.Fatal("invalid expected solve accepted")
		}
	}
	for _, platform := range []string{"", "linux/amd64,linux/arm64", "darwin/amd64"} {
		if _, err := parseBaseResolutionMetadata(baseResolutionFixture(platform, `[]`), "solve_1-abc", platform, baseResolutionTestRoot); err == nil {
			t.Fatal("invalid expected platform accepted")
		}
	}
	zero := "sha256:" + strings.Repeat("0", 64)
	payload := strings.ReplaceAll(string(baseResolutionFixture("linux/amd64", `[]`)), baseResolutionTestRoot, zero)
	if _, err := parseBaseResolutionMetadata([]byte(payload), "solve_1-abc", "linux/amd64", zero); err == nil {
		t.Fatal("zero expected root accepted")
	}
}

func TestBaseResolutionAcceptsBoundaryRefAndIndentedBuildctlRecord(t *testing.T) {
	ref := strings.Repeat("a", 128)
	payload := strings.ReplaceAll(string(baseResolutionFixture("linux/amd64", `[]`)), "solve_1-abc", ref)
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &document); err != nil {
		t.Fatal(err)
	}
	pretty, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := parseBaseResolutionMetadata(pretty, ref, "linux/amd64", baseResolutionTestRoot)
	if err != nil || evidence.JSON() == "" {
		t.Fatalf("valid indented boundary record rejected: %v", err)
	}
}
