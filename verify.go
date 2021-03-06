// TODO: The various grouping and verification functions should probably be in
// a separate file and it should be obvious how to adjust or write a new one.

//go:generate stringer -type=Status,Reason -output verify_string.go verify.go
package skate

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"git.archive.org/martin/cgraph/skate/set"
	"git.archive.org/martin/cgraph/skate/zipkey"
)

// This file is a port of fuzzycat.verify to Go.

type (
	// Status represents match strength.
	Status int
	// Reason gives more context to status result.
	Reason int
)

const (
	StatusUnknown Status = iota
	StatusExact
	StatusStrong
	StatusWeak
	StatusDifferent
	StatusAmbiguous

	ReasonUnknown Reason = iota
	ReasonAppendix
	ReasonArxiv
	ReasonArxivVersion
	ReasonBlacklisted
	ReasonBlacklistedFragment
	ReasonBookChapter
	ReasonChemFormula
	ReasonComponent
	ReasonContainer
	ReasonContainerNameBlacklist
	ReasonContribIntersectionEmpty
	ReasonCustomBSISubdoc
	ReasonCustomBSIUndated
	ReasonCustomIEEEArxiv
	ReasonCustomIOPMAPattern
	ReasonCustomPrefix1014288
	ReasonCustomPrefix105860ChoiceReview
	ReasonCustomPrefix107916
	ReasonCustomVHS
	ReasonDOI
	ReasonDataciteRelatedID
	ReasonDataciteVersion
	ReasonDatasetDOI
	ReasonFigshareVersion
	ReasonJaccardAuthors
	ReasonJstorID
	ReasonMaxClusterSizeExceeded
	ReasonNumDiff
	ReasonPMCID
	ReasonPMID
	ReasonPMIDDOIPair
	ReasonPageCount
	ReasonPreprintPublished
	ReasonPublisherBlacklist
	ReasonReleaseType
	ReasonSharedDOIPrefix
	ReasonShortTitle
	ReasonSingularCluster
	ReasonSlugTitleAuthorMatch
	ReasonSubtitle
	ReasonTitleArtifact
	ReasonTitleAuthorMatch
	ReasonTitleFilename
	ReasonTokenizedAuthors
	ReasonVersionedDOI
	ReasonWorkID
	ReasonYear
)

// Short name.
func (s Status) Short() string {
	return strings.ToLower(strings.Replace(s.String(), "Status", "", 1))
}

// Short name.
func (r Reason) Short() string {
	return strings.ToLower(strings.Replace(r.String(), "Reason", "", 1))
}

// MatchResult is the result of a verification.
type MatchResult struct {
	Status Status
	Reason Reason
}

var (
	PatAppendix        = regexp.MustCompile(`appendix ?[^ ]*$`)
	PatFigshareVersion = regexp.MustCompile(`[.]v[0-9]+$`)
	PatVersionedDOI    = regexp.MustCompile(`10[.].*/v[0-9]{1,}$`)
	PatArxivVersion    = regexp.MustCompile(`(.*)v[0-9]{1,2}$`)
	PatFilenameLike    = regexp.MustCompile(`.*[.][a-z]{2,3}$`)
	PatDigits          = regexp.MustCompile(`\d+`)
	PatPages           = regexp.MustCompile(`([0-9]{1,})-([0-9]{1,})`)
)

// XXX: add all pairs verification (e.g. self-match).

// RefCluster deserialized a single cluster document and returns a tabular file
// with identifiers, match status and reason.
func RefCluster(p []byte) ([]byte, error) {
	var (
		cr  *ClusterResult
		buf bytes.Buffer
	)
	if err := json.Unmarshal(p, &cr); err != nil {
		return nil, err
	}
	pivot, err := cr.OneNonRef()
	if err != nil {
		return nil, err
	}
	for _, re := range cr.Values {
		if re.Extra.Skate.Status != "ref" {
			continue
		}
		result := Verify(pivot, re, 5)
		if _, err := fmt.Fprintf(&buf, "%s %s %s %s\n",
			pivot.Ident, re.Ident, result.Status, result.Reason); err != nil {
			return nil, err
		}
		// XXX: We can generate a biblioref here, too.
	}
	return buf.Bytes(), nil
}

// RefClusterToBiblioRef creates a BiblioRef schema from exact and strong matches.
func RefClusterToBiblioRef(p []byte) ([]byte, error) {
	var (
		cr  *ClusterResult
		br  *BiblioRef
		buf bytes.Buffer
	)
	if err := json.Unmarshal(p, &cr); err != nil {
		return nil, err
	}
	pivot, err := cr.OneNonRef()
	if err != nil {
		return nil, err
	}
	for _, re := range cr.Values {
		if re.Extra.Skate.Status != "ref" {
			continue
		}
		result := Verify(pivot, re, 5)
		switch result.Status {
		case StatusExact, StatusStrong:
			if result.Reason == ReasonDOI {
				// Assume we already have the DOI matches.
				continue
			}
			br = generateBiblioRef(re, pivot, result.Status, result.Reason, "fuzzy")
			b, err := json.Marshal(br)
			if err != nil {
				return nil, err
			}
			b = append(b, []byte("\n")...)
			return b, nil
		default:
			continue
		}
	}
	return buf.Bytes(), nil
}

// generateBiblioRef generates a bibliographic schema document.
func generateBiblioRef(source, target *Release, matchStatus Status, matchReason Reason, provenance string) *BiblioRef {
	var bref BiblioRef
	bref.SourceReleaseIdent = source.Ident
	bref.SourceWorkIdent = source.WorkID
	bref.SourceReleaseStage = source.ReleaseStage
	if source.ReleaseYear() > 1000 {
		bref.SourceYear = source.ReleaseYearString()
	}
	bref.RefIndex = source.Extra.Skate.Ref.Index
	bref.RefKey = source.Extra.Skate.Ref.Key
	bref.TargetReleaseIdent = target.Ident
	bref.TargetWorkIdent = target.WorkID
	bref.MatchProvenance = provenance
	bref.MatchStatus = matchStatus.Short()
	bref.MatchReason = matchReason.Short()
	return &bref
}

// ZipUnverified takes a release and refs reader (tsv, with ident, key, doc)
// and assigns a fixed match result.
func ZipUnverified(releases, refs io.Reader, mr MatchResult, provenance string, w io.Writer) error {
	// Define a grouper, working on one set of refs and releases with the same
	// key at a time. Here, we do verification and write out the generated
	// biblioref.
	enc := json.NewEncoder(w)
	keyer := func(s string) (string, error) {
		if k := lineColumn(s, "\t", 2); k == "" {
			return k, fmt.Errorf("cannot get key: %s", s)
		} else {
			return k, nil
		}
	}
	grouper := func(g *zipkey.Group) error {
		if len(g.G0) == 0 || len(g.G1) == 0 {
			return nil
		}
		target, err := stringToRelease(lineColumn(g.G0[0], "\t", 3))
		if err != nil {
			return err
		}
		for _, line := range g.G1 {
			ref, err := stringToRef(lineColumn(line, "\t", 3))
			if err != nil {
				return err
			}
			var bref BiblioRef
			bref.SourceReleaseIdent = ref.ReleaseIdent
			bref.SourceWorkIdent = ref.WorkIdent
			bref.SourceReleaseStage = ref.ReleaseStage
			bref.SourceYear = fmt.Sprintf("%d", ref.ReleaseYear)
			bref.RefIndex = ref.Index + 1 // we want 1-index (also helps with omitempty)
			bref.RefKey = ref.Key
			bref.TargetReleaseIdent = target.Ident
			bref.TargetWorkIdent = target.WorkID
			bref.MatchProvenance = provenance
			bref.MatchStatus = mr.Status.Short()
			bref.MatchReason = mr.Reason.Short()
			if err := enc.Encode(bref); err != nil {
				return err
			}
		}
		return nil
	}
	zipper := zipkey.New(releases, refs, keyer, grouper)
	return zipper.Run()
}

// ZipVerifyRefs takes a release and refs reader (tsv, with ident, key, doc)
// and will execute gf for each group found.
func ZipVerifyRefs(releases, refs io.Reader, w io.Writer) error {
	// Define a grouper, working on one set of refs and releases with the same
	// key at a time. Here, we do verification and write out the generated
	// biblioref.
	enc := json.NewEncoder(w)
	keyer := func(s string) (string, error) {
		if k := lineColumn(s, "\t", 2); k == "" {
			return k, fmt.Errorf("cannot get key: %s", s)
		} else {
			return k, nil
		}
	}
	grouper := func(g *zipkey.Group) error {
		if len(g.G0) == 0 || len(g.G1) == 0 {
			return nil
		}
		pivot, err := stringToRelease(lineColumn(g.G0[0], "\t", 3))
		if err != nil {
			return err
		}
		for _, line := range g.G1 {
			re, err := stringToRelease(lineColumn(line, "\t", 3))
			if err != nil {
				return err
			}
			result := Verify(pivot, re, 5)
			switch result.Status {
			case StatusExact, StatusStrong:
				if result.Reason == ReasonDOI {
					continue
				}
				br := generateBiblioRef(re, pivot, result.Status, result.Reason, "fuzzy")
				if err := enc.Encode(br); err != nil {
					return err
				}
			}
		}
		return nil
	}
	zipper := zipkey.New(releases, refs, keyer, grouper)
	return zipper.Run()
}

// lineColumn returns a specific column (1-indexed, like cut) from a tabular
// file, returns empty string if column is invalid.
func lineColumn(line, sep string, column int) string {
	var parts = strings.Split(strings.TrimSpace(line), sep)
	if len(parts) < column {
		return ""
	} else {
		return parts[column-1]
	}
}

func stringToRelease(s string) (r *Release, err error) {
	err = json.Unmarshal([]byte(s), &r)
	return
}

func stringToRef(s string) (r *Ref, err error) {
	err = json.Unmarshal([]byte(s), &r)
	return
}

// Verify follows the fuzzycat (Python) implementation of this function: it
// compares two release entities. The Go version can be used for large batch
// processing (where the Python version might take two or more days).
func Verify(a, b *Release, minTitleLength int) MatchResult {
	if a.ExtIDs.DOI != "" && a.ExtIDs.DOI == b.ExtIDs.DOI {
		return MatchResult{StatusExact, ReasonDOI}
	}
	if a.WorkID != "" && a.WorkID == b.WorkID {
		return MatchResult{StatusExact, ReasonWorkID}
	}
	aTitleLower := strings.ToLower(a.Title)
	bTitleLower := strings.ToLower(b.Title)
	if utf8.RuneCountInString(a.Title) < minTitleLength {
		return MatchResult{StatusAmbiguous, ReasonShortTitle}
	}
	if BlacklistTitle.Contains(aTitleLower) {
		return MatchResult{StatusAmbiguous, ReasonBlacklisted}
	}
	if BlacklistTitle.Contains(bTitleLower) {
		return MatchResult{StatusAmbiguous, ReasonBlacklisted}
	}
	for _, fragment := range BlacklistTitleFragments.Slice() {
		if strings.Contains(aTitleLower, fragment) {
			return MatchResult{StatusAmbiguous, ReasonBlacklistedFragment}
		}
	}
	if strings.Contains(aTitleLower, "subject index") && strings.Contains(bTitleLower, "subject index") {
		if a.ContainerID != "" && a.ContainerID != b.ContainerID {
			return MatchResult{StatusDifferent, ReasonContainer}
		}
	}
	if a.Title != "" && a.Title == b.Title &&
		a.Extra.DataCite.MetadataVersion > 0 && b.Extra.DataCite.MetadataVersion > 0 &&
		a.Extra.DataCite.MetadataVersion != b.Extra.DataCite.MetadataVersion {
		return MatchResult{StatusExact, ReasonDataciteVersion}
	}
	if strings.HasPrefix(a.ExtIDs.DOI, "10.14288/") && strings.HasPrefix(b.ExtIDs.DOI, "10.14288/") &&
		a.ExtIDs.DOI != b.ExtIDs.DOI {
		return MatchResult{StatusDifferent, ReasonCustomPrefix1014288}
	}
	if strings.HasPrefix(a.ExtIDs.DOI, "10.3403") && strings.HasPrefix(b.ExtIDs.DOI, "10.3403") {
		if a.ExtIDs.DOI+"u" == b.ExtIDs.DOI || b.ExtIDs.DOI+"u" == a.ExtIDs.DOI {
			return MatchResult{StatusStrong, ReasonCustomBSIUndated}
		}
		aSubtitle := a.Subtitle()
		bSubtitle := b.Subtitle()
		if a.Title != "" && a.Title == b.Title &&
			((len(aSubtitle) > 0 && aSubtitle[0] != "" && len(bSubtitle) == 0) ||
				(len(aSubtitle) == 0 && len(bSubtitle) > 0 && bSubtitle[0] != "")) {
			return MatchResult{StatusStrong, ReasonCustomBSISubdoc}
		}
	}
	if strings.HasPrefix(a.ExtIDs.DOI, "10.1149") && strings.HasPrefix(b.ExtIDs.DOI, "10.1149") {
		v := "10.1149/ma"
		if (strings.HasPrefix(a.ExtIDs.DOI, v) && !strings.HasPrefix(b.ExtIDs.DOI, v)) ||
			(!strings.HasPrefix(a.ExtIDs.DOI, v) && strings.HasPrefix(b.ExtIDs.DOI, v)) {
			return MatchResult{StatusDifferent, ReasonCustomIOPMAPattern}
		}
	}
	if strings.Contains(a.Title, "Zweckverband Volkshochschule") && a.Title != b.Title {
		return MatchResult{StatusDifferent, ReasonCustomVHS}
	}
	if PatAppendix.MatchString(a.Title) {
		return MatchResult{StatusAmbiguous, ReasonAppendix}
	}
	if strings.HasPrefix(a.ExtIDs.DOI, "10.6084/") && strings.HasPrefix(b.ExtIDs.DOI, "10.6084/") {
		av := PatFigshareVersion.ReplaceAllString(a.ExtIDs.DOI, "")
		bv := PatFigshareVersion.ReplaceAllString(b.ExtIDs.DOI, "")
		if av == bv {
			return MatchResult{StatusStrong, ReasonFigshareVersion}
		}
	}
	if PatVersionedDOI.MatchString(a.ExtIDs.DOI) && PatVersionedDOI.MatchString(b.ExtIDs.DOI) {
		return MatchResult{StatusStrong, ReasonVersionedDOI}
	}
	if looksLikeComponent(a.ExtIDs.DOI, b.ExtIDs.DOI) {
		return MatchResult{StatusStrong, ReasonVersionedDOI}
	}
	if len(a.Extra.DataCite.Relations) > 0 || len(b.Extra.DataCite.Relations) > 0 {
		getRelatedDOI := func(rel *Release) *set.Set {
			ss := set.New()
			for _, rel := range rel.Extra.DataCite.Relations {
				if strings.ToLower(rel.RelatedIdentifierType) != "doi" {
					continue
				}
				ss.Add(rel.RelatedIdentifier())
			}
			return ss
		}
		aRelated := getRelatedDOI(a)
		bRelated := getRelatedDOI(b)
		if aRelated.Contains(b.ExtIDs.DOI) || bRelated.Contains(a.ExtIDs.DOI) {
			return MatchResult{StatusStrong, ReasonDataciteRelatedID}
		}
	}
	if a.ExtIDs.Arxiv != "" && b.ExtIDs.Arxiv != "" {
		aSub := PatArxivVersion.FindStringSubmatch(a.ExtIDs.Arxiv)
		bSub := PatArxivVersion.FindStringSubmatch(b.ExtIDs.Arxiv)
		if len(aSub) == 2 && len(bSub) == 2 && aSub[1] == bSub[1] {
			return MatchResult{StatusStrong, ReasonArxivVersion}
		}
	}
	if a.ReleaseType != b.ReleaseType {
		types := set.FromSlice([]string{a.ReleaseType, b.ReleaseType})
		ignoreTypes := set.FromSlice([]string{"article", "article-journal", "report", "paper-conference"})
		if types.Intersection(ignoreTypes).IsEmpty() {
			return MatchResult{StatusDifferent, ReasonReleaseType}
		}
		if types.Contains("dataset") && (types.Contains("article") || types.Contains("article-journal")) {
			return MatchResult{StatusDifferent, ReasonReleaseType}
		}
		if types.Contains("book") && (types.Contains("article") || types.Contains("article-journal")) {
			return MatchResult{StatusDifferent, ReasonReleaseType}
		}
	}
	if a.ReleaseType == "dataset" && b.ReleaseType == "dataset" && a.ExtIDs.DOI != b.ExtIDs.DOI {
		return MatchResult{StatusDifferent, ReasonDatasetDOI}
	}
	if a.ReleaseType == "chapter" && b.ReleaseType == "chapter" &&
		a.Extra.ContainerName != "" && a.Extra.ContainerName != b.Extra.ContainerName {
		return MatchResult{StatusDifferent, ReasonBookChapter}
	}
	if a.Extra.Crossref.Type == "component" && a.Title != b.Title {
		return MatchResult{StatusDifferent, ReasonComponent}
	}
	if a.ReleaseType == "component" && b.ReleaseType == "component" {
		if a.ExtIDs.DOI != "" && a.ExtIDs.DOI != b.ExtIDs.DOI {
			return MatchResult{StatusDifferent, ReasonComponent}
		}
	}
	aSlugTitle := strings.TrimSpace(strings.Replace(slugifyString(a.Title), "\n", " ", -1))
	bSlugTitle := strings.TrimSpace(strings.Replace(slugifyString(b.Title), "\n", " ", -1))

	if aSlugTitle == bSlugTitle {
		if a.ReleaseYear() != 0 && b.ReleaseYear() != 0 && absInt(a.ReleaseYear()-b.ReleaseYear()) > 40 {
			return MatchResult{StatusDifferent, ReasonYear}
		}
	}
	if aSlugTitle == bSlugTitle {
		ieeeArxivCheck := func(a, b *Release) (ok bool) {
			return doiPrefix(a.ExtIDs.DOI) == "10.1109" && b.ExtIDs.Arxiv != ""
		}
		if ieeeArxivCheck(a, b) || ieeeArxivCheck(b, a) {
			return MatchResult{StatusStrong, ReasonCustomIEEEArxiv}
		}
	}
	if aSlugTitle == bSlugTitle {
		if strings.HasPrefix(a.ExtIDs.DOI, "10.7916/") && strings.HasPrefix(b.ExtIDs.DOI, "10.7916/") {
			return MatchResult{StatusAmbiguous, ReasonCustomPrefix107916}
		}
	}
	if aSlugTitle == bSlugTitle {
		aSubtitle := a.Subtitle()
		bSubtitle := b.Subtitle()
		for _, aSub := range aSubtitle {
			for _, bSub := range bSubtitle {
				if slugifyString(aSub) != slugifyString(bSub) {
					return MatchResult{StatusDifferent, ReasonSubtitle}
				}
			}
		}
	}
	rawAuthors := func(rel *Release) (names []string) {
		for _, c := range rel.Contribs {
			name := strings.TrimSpace(c.RawName)
			if name == "" {
				continue
			}
			names = append(names, name)
		}
		return names
	}
	aAuthors := set.FromSlice(rawAuthors(a))
	bAuthors := set.FromSlice(rawAuthors(b))
	aSlugAuthors := set.FromSlice(mapString(slugifyString, aAuthors.Slice()))
	bSlugAuthors := set.FromSlice(mapString(slugifyString, bAuthors.Slice()))
	if aTitleLower == bTitleLower {
		if aAuthors.Len() > 0 && aAuthors.Equals(bAuthors) {
			if a.ReleaseYear() > 0 && b.ReleaseYear() > 0 && absInt(a.ReleaseYear()-b.ReleaseYear()) > 4 {
				return MatchResult{StatusDifferent, ReasonYear}
			}
			return MatchResult{StatusExact, ReasonTitleAuthorMatch}
		}
	}
	if looksLikeFilename(a.Title) || looksLikeFilename(b.Title) {
		if a.Title != b.Title {
			return MatchResult{StatusDifferent, ReasonTitleFilename}
		}
	}
	if a.Title != "" && a.Title == b.Title {
		if a.ReleaseYear() > 0 && b.ReleaseYear() > 0 && absInt(a.ReleaseYear()-b.ReleaseYear()) > 2 {
			return MatchResult{StatusDifferent, ReasonYear}
		}
	}
	// XXX: skipping chemical formula detection (to few cases; https://git.io/Jtdax)
	if len(aSlugTitle) < 10 && aSlugTitle != bSlugTitle {
		return MatchResult{StatusAmbiguous, ReasonShortTitle}
	}
	if PatDigits.MatchString(aSlugTitle) &&
		aSlugTitle != bSlugTitle &&
		unifyDigits(aSlugTitle) == unifyDigits(bSlugTitle) {
		return MatchResult{StatusDifferent, ReasonNumDiff}
	}
	if aSlugTitle != "" && bSlugTitle != "" &&
		strings.ReplaceAll(aSlugTitle, " ", "") == strings.ReplaceAll(bSlugTitle, " ", "") {
		if aSlugAuthors.Intersection(bSlugAuthors).Len() > 0 {
			if a.ReleaseYear() > 0 && b.ReleaseYear() > 0 && absInt(a.ReleaseYear()-b.ReleaseYear()) > 4 {
				return MatchResult{StatusDifferent, ReasonYear}
			}
			return MatchResult{StatusStrong, ReasonSlugTitleAuthorMatch}
		}
	}
	if a.ReleaseYear() > 0 && a.ReleaseYear() == b.ReleaseYear() && aTitleLower == bTitleLower {
		if (a.ExtIDs.PMID != "" && b.ExtIDs.DOI != "") || (b.ExtIDs.PMID != "" && a.ExtIDs.DOI != "") {
			return MatchResult{StatusStrong, ReasonPMIDDOIPair}
		}
	}
	if a.ExtIDs.Jstor != "" && b.ExtIDs.Jstor != "" && a.ExtIDs.Jstor != b.ExtIDs.Jstor {
		return MatchResult{StatusDifferent, ReasonJstorID}
	}
	if a.ContainerID != "" && a.ContainerID == b.ContainerID && a.ExtIDs.DOI != b.ExtIDs.DOI &&
		doiPrefix(a.ExtIDs.DOI) != "10.1126" &&
		doiPrefix(a.ExtIDs.DOI) == doiPrefix(b.ExtIDs.DOI) {
		return MatchResult{StatusDifferent, ReasonSharedDOIPrefix}
	}
	if aAuthors.Len() > 0 && aSlugAuthors.Intersection(bSlugAuthors).IsEmpty() {
		numAuthors := set.Min(aSlugAuthors, bSlugAuthors)
		score := averageScore(aSlugAuthors, bSlugAuthors)
		if (numAuthors < 3 && score > 0.9) || (numAuthors >= 3 && score > 0.5) {
			return MatchResult{StatusStrong, ReasonTokenizedAuthors}
		}
		aTok := set.FromSlice(strings.Fields(aSlugAuthors.Join(" ")))
		bTok := set.FromSlice(strings.Fields(bSlugAuthors.Join(" ")))
		aTok = set.Filter(aTok, func(s string) bool {
			return len(s) > 2
		})
		bTok = set.Filter(bTok, func(s string) bool {
			return len(s) > 2
		})
		if aTok.Len() > 0 && bTok.Len() > 0 {
			if aTok.Jaccard(bTok) > 0.35 {
				return MatchResult{StatusStrong, ReasonJaccardAuthors}
			}
		}
		return MatchResult{StatusDifferent, ReasonContribIntersectionEmpty}
	}
	if doiPrefix(a.ExtIDs.DOI) == "10.5860" || doiPrefix(b.ExtIDs.DOI) == "10.5860" {
		return MatchResult{StatusAmbiguous, ReasonCustomPrefix105860ChoiceReview}
	}
	// XXX: parse pages
	aParsedPages := parsePageString(a.Pages)
	bParsedPages := parsePageString(b.Pages)
	if aParsedPages.Err != nil && bParsedPages.Err != nil {
		if absInt(aParsedPages.Count()-bParsedPages.Count()) > 5 {
			return MatchResult{StatusDifferent, ReasonPageCount}
		}
	}
	if aAuthors.Equals(bAuthors) &&
		a.ContainerID == b.ContainerID &&
		a.ReleaseYear() == b.ReleaseYear() &&
		a.Title != b.Title &&
		(strings.Contains(a.Title, b.Title) || strings.Contains(b.Title, a.Title)) {
		return MatchResult{StatusStrong, ReasonTitleArtifact}
	}
	return MatchResult{
		StatusAmbiguous,
		ReasonUnknown,
	}
}

type ParsedPages struct {
	Start int
	End   int
	Err   error
}

func (pp *ParsedPages) Count() int {
	return pp.End - pp.Start + 1
}

func parsePageString(s string) *ParsedPages {
	s = strings.TrimSpace(s)
	var pp = ParsedPages{}
	if len(s) == 0 {
		pp.Err = fmt.Errorf("parse pages: empty string")
		return &pp
	}
	matches := PatPages.FindStringSubmatch(s)
	if len(matches) != 3 {
		pp.Err = fmt.Errorf("parse pages: no page pattern")
		return &pp
	}
	start, end := matches[1], matches[2]
	if len(end) == 1 && len(start) > 1 && start[len(start)-1] < end[0] {
		end = fmt.Sprintf("%s%c", start[:len(start)-1], end[0])
	}
	if pp.Start, pp.Err = strconv.Atoi(start); pp.Err != nil {
		return &pp
	}
	if pp.End, pp.Err = strconv.Atoi(end); pp.Err != nil {
		return &pp
	}
	if pp.Start > pp.End {
		pp.Err = fmt.Errorf("invalid page count: %s", s)
	}
	return &pp
}

// averageScore take a limited set of authors and calculates pairwise
// similarity scores, then returns the average of the best scores; between 0
// and 1.
func averageScore(a, b *set.Set) float64 {
	aTrimmed := a.TopK(5)
	bTrimmed := b.TopK(5)
	maxScores := make(map[string]float64) // For each a, keep the max.
	for _, pair := range aTrimmed.Product(bTrimmed) {
		a, b := pair[0], pair[1]
		score := authorSimilarityScore(a, b)
		if v, ok := maxScores[a]; !ok || score > v {
			maxScores[a] = score
		}
	}
	var sum, avg float64
	for _, v := range maxScores {
		sum += v
	}
	avg = sum / float64(len(maxScores))
	return avg
}

// authorSimilarityScore is a hacky similarity score.
func authorSimilarityScore(s, t string) float64 {
	ss := set.FromSlice(tokenNgrams(s, 2))
	ts := set.FromSlice(tokenNgrams(t, 2))
	return ss.Jaccard(ts)
}

// tokenNgrams are groups of n tokens per token in string, e.g. for n=2 and
// string "Anne K Lam", we would get ["an", "ne", "k", "la", "m"].
func tokenNgrams(s string, n int) (result []string) {
	var buf bytes.Buffer
	for _, token := range tokenizeString(s) {
		buf.Reset()
		for i, c := range token {
			if i > 0 && i%n == 0 {
				result = append(result, buf.String())
				buf.Reset()
			}
			buf.WriteRune(c) // XXX: skipping error handling
		}
		result = append(result, buf.String())
	}
	return
}

func tokenizeString(s string) []string {
	return strings.Fields(strings.ToLower(s))
}

func doiPrefix(s string) string {
	parts := strings.Split(s, "/")
	return parts[0]
}

// unifyDigits replaces all digit groups with a placeholder, e.g. "<NUM>".
func unifyDigits(s string) string {
	return PatDigits.ReplaceAllString(s, "<NUM>")
}

// looksLikeFilename returns true, if the given string could be a filename.
func looksLikeFilename(s string) bool {
	if len(strings.Fields(s)) > 1 {
		return false
	}
	return PatFilenameLike.MatchString(s)
}

// mapString applies a function on each element of a string slice.
func mapString(f func(string) string, vs []string) (result []string) {
	for _, v := range vs {
		result = append(result, f(v))
	}
	return result
}

// absInt returns the absolute value of an int.
func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// slugifyString is a basic string slugifier.
func slugifyString(s string) string {
	var buf bytes.Buffer
	for _, c := range strings.TrimSpace(strings.ToLower(s)) {
		if (c > 96 && c < 123) || (c > 47 && c < 58) || (c == 32) || (c == 9) || (c == 10) {
			fmt.Fprintf(&buf, "%c", c)
		}
	}
	return strings.Join(strings.Fields(buf.String()), " ")
}

// looksLikeComponent returns true, if either a looks like a component of b, or vice versa.
func looksLikeComponent(a, b string) bool {
	ac := strings.Split(a, ".")
	bc := strings.Split(b, ".")
	if len(ac) > 1 {
		if strings.Join(ac[0:len(ac)-1], ".") == b {
			return true
		}
	}
	if len(bc) > 1 {
		if strings.Join(bc[0:len(bc)-1], ".") == a {
			return true
		}
	}
	return false
}
