package skate

import "strings"

type Status int

const (
	StatusUnknown Status = iota
	StatusDifferent
	StatusExact
	StatusStrong
	StatusWeak
	StatusAmbiguous
)

type Reason int

const (
	ReasonAppendix Reason = iota
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
	ReasonCustomIOPMAPattern
	ReasonCustomIEEEArxiv
	ReasonCustomPrefix_10_14288
	ReasonCustomPrefix_10_5860_Choice_Review
	ReasonCustomPrefix_10_7916
	ReasonCustomVHS
	ReasonDOI
	ReasonDataciteRelatedID
	ReasonDataciteVersion
	ReasonDatasetDOI
	ReasonFigshareVersion
	ReasonJaccardAuthors
	ReasonJstorId
	ReasonMaxClusterSizeExceeded
	ReasonNumDiff
	ReasonPageCount
	ReasonPmidDoiPair
	ReasonPreprintPublished
	ReasonPublisherBlacklist
	ReasonReleaseType
	ReasonSharedDoiPrefix
	ReasonShortTitle
	ReasonSingularCluster
	ReasonSlugTitleAuthorMatch
	ReasonSubtitle
	ReasonTitleArtifact
	ReasonTitleAuthorMatch
	ReasonTitleFilename
	ReasonTokenizedAuthors
	ReasonUnknown
	ReasonVersionedDOI
	ReasonWorkID
	ReasonYear
)

type VerifyStatus struct {
	Status Status
	Reason Reason
}

// Verify is a stub implementation of the rules found in fuzzycat.
func Verify(a, b Release, minTitleLength int) VerifyStatus {
	if a.ExtIDs.DOI != "" && a.ExtIDs.DOI == b.ExtIDs.DOI {
		return VerifyStatus{StatusExact, ReasonDOI}
	}
	if a.WorkID != "" && a.WorkID == b.WorkID {
		return VerifyStatus{StatusExact, ReasonWorkID}
	}
	if len(a.Title) < minTitleLength {
		return VerifyStatus{StatusAmbiguous, ReasonShortTitle}
	}
	aTitleLower := strings.ToLower(a.Title)
	bTitleLower := strings.ToLower(b.Title)
	if BlacklistTitle.Contains(aTitleLower) {
		return VerifyStatus{StatusAmbiguous, ReasonBlacklisted}
	}
	// XXX: fix
	for _, fragment := range BlacklistTitleFragments {
		if strings.Contains(aTitleLower, fragment) {
			return VerifyStatus{StatusAmbiguous, ReasonBlacklistedFragment}
		}
	}
	if inAll("subject index", []string{aTitleLower, bTitleLower}) {
		if a.ContainerID != b.ContainerID {
			return VerifyStatus{StatusDifferent, ReasonContainer}
		}
	}
	if a.Title != "" && a.Title == b.Title &&
		a.Extra.DataCite.MetadataVersion != b.Extra.DataCite.MetadataVersion {
	}
	// XXX: extend, then deploy and run against the 90G cluster file

	return VerifyStatus{StatusUnknown, ReasonUnknown}
}

// containsAny returns true, if any value from vs appears in v.
func containsAny(v string, vs []string) bool {
	for _, u := range vs {
		if strings.Contains(v, u) {
			return true
		}
	}
	return false
}

func inAll(v string, vs []string) bool {
	for _, w := range vs {
		if !strings.Contains(w, v) {
			return false
		}
	}
	return true
}
