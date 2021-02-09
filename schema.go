package skate

import "strconv"

// RefToRelease converts a ref to a release. Set a extra.skate.status flag to
// be able to distinguish coverted entities later.
func RefToRelease(ref *Ref) (*Release, error) {
	var (
		release  Release
		b        = ref.Biblio
		contribs = make([]struct {
			Index   int    `json:"index,omitempty"`
			RawName string `json:"raw_name,omitempty"`
			Role    string `json:"role,omitempty"`
		}, len(b.ContribRawNames))
	)
	release.Ident = ref.ReleaseIdent
	release.WorkID = ref.WorkIdent
	release.ExtIDs.Arxiv = b.ArxivId
	release.ExtIDs.DOI = b.DOI
	release.ExtIDs.PMID = b.PMID
	release.ExtIDs.PMCID = b.PMCID
	release.Title = b.Title
	release.Publisher = b.Publisher
	release.ContainerName = b.ContainerName
	release.Volume = b.Volume
	release.Issue = b.Issue
	release.Pages = b.Pages
	release.ReleaseYear = strconv.Itoa(ref.ReleaseYear)
	for i, name := range b.ContribRawNames {
		contribs[i].Index = i
		contribs[i].RawName = name
	}
	release.Contribs = contribs
	return &release, nil
}

// Ref is a reference document.
type Ref struct {
	Biblio struct {
		ArxivId         string   `json:"arxiv_id,omitempty"`
		ContainerName   string   `json:"container_name,omitempty"`
		ContribRawNames []string `json:"contrib_raw_names,omitempty"`
		DOI             string   `json:"doi,omitempty"`
		Issue           string   `json:"issue,omitempty"`
		PMCID           string   `json:"pmcid,omitempty"`
		PMID            string   `json:"pmid,omitempty"`
		Pages           string   `json:"pages,omitempty"`
		Publisher       string   `json:"publisher,omitempty"`
		Title           string   `json:"title,omitempty"`
		Unstructured    string   `json:"unstructured,omitempty"`
		Url             string   `json:"url,omitempty"`
		Volume          string   `json:"volume,omitempty"`
		Year            int64    `json:"year,omitempty"`
	} `json:"biblio"`
	Index        int64  `json:"index,omitempty"`
	Key          string `json:"key,omitempty"`
	RefSource    string `json:"ref_source,omitempty"`
	ReleaseYear  int    `json:"release_year,omitempty"`
	ReleaseIdent string `json:"release_ident,omitempty"`
	WorkIdent    string `json:"work_ident,omitempty"`
}

// Release document.
type Release struct {
	ContainerName string `json:"container_name,omitempty"`
	Contribs      []struct {
		Index   int    `json:"index,omitempty"`
		RawName string `json:"raw_name,omitempty"`
		Role    string `json:"role,omitempty"`
	} `json:"contribs,omitempty"`
	ExtIDs struct {
		DOI         string `json:"doi,omitempty"`
		PMID        string `json:"pmid,omitempty"`
		PMCID       string `json:"pmcid,omitempty"`
		Arxiv       string `json:"arxiv,omitempty"`
		Core        string `json:"core,omitempty"`
		WikidataQID string `json:"wikidata_qid,omitempty"`
		Jstor       string `json:"jstor,omitempty"`
	} `json:"ext_ids,omitempty"`
	Ident       string `json:"ident,omitempty"`
	Publisher   string `json:"publisher,omitempty"`
	ReleaseDate string `json:"release_date,omitempty"`
	ReleaseYear string `json:"release_year,omitempty"`
	Issue       string `json:"issue,omitempty"`
	Volume      string `json:"volume,omitempty"`
	Pages       string `json:"pages,omitempty"`
	Title       string `json:"title,omitempty"`
	WorkID      string `json:"work_id,omitempty"`
	Extra       struct {
		Skate struct {
			// Mark as converted.
			Status string `json:"status,omitempty"`
		} `json:"skate,omitempty"`
	} `json:"extra,omitempty"`
}

// BiblioRef as a prototype for indexing.
type BiblioRef struct {
	Key                    string `json:"_key,omitempty"`
	UpdateTs               string `json:"update_ts,omitempty"`
	SourceReleaseIdent     string `json:"source_release_ident,omitempty"`
	SourceWorkIdent        string `json:"source_work_ident,omitempty"`
	SourceWikipediaArticle string `json:"source_wikipedia_article,omitempty"`
	SourceReleaseStage     string `json:"source_release_stage,omitempty"`
	SourceYear             string `json:"source_year,omitempty"`
	RefIndex               int    `json:"ref_index,omitempty"`
	RefKey                 string `json:"ref_key,omitempty"`
	RefLocator             string `json:"ref_locator,omitempty"`
	TargetReleaseIdent     string `json:"target_release_ident"`
	TargetWorkIdent        string `json:"target_work_ident"`
	TargetOpenLibraryWork  string `json:"target_openlibrary_work"`
	TargetURLSurt          string `json:"target_url_surt"`
	TargetURL              string `json:"target_url"`
	MatchProvenance        string `json:"match_provenance"`
	MatchStatus            string `json:"match_status"`
	MatchReason            string `json:"match_reason"`
	TargetUnstructured     string `json:"target_unstructured"`
	TargetCSL              string `json:"target_csl"`
}
