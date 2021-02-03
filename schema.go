package skate

// RefToRelease converts a ref to a release.
func RefToRelease(ref *Ref) (*Release, error) {
	var (
		release  Release
		b        = ref.Biblio
		contribs = make([]struct {
			Index   int64  `json:"index"`
			RawName string `json:"raw_name"`
			Role    string `json:"role"`
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
	release.ReleaseYear = b.Year
	for i, name := range b.ContribRawNames {
		contribs[i].RawName = name
	}
	release.Contribs = contribs
	return &release, nil
}

// Ref is a reference document.
type Ref struct {
	Biblio struct {
		ArxivId         string   `json:"arxiv_id"`
		ContainerName   string   `json:"container_name"`
		ContribRawNames []string `json:"contrib_raw_names"`
		DOI             string   `json:"doi"`
		Issue           string   `json:"issue"`
		Pages           string   `json:"pages"`
		PMCID           string   `json:"pmcid"`
		PMID            string   `json:"pmid"`
		Publisher       string   `json:"publisher"`
		Title           string   `json:"title"`
		Unstructured    string   `json:"unstructured"`
		Url             string   `json:"url"`
		Volume          string   `json:"volume"`
		Year            string   `json:"year"`
	} `json:"biblio"`
	Index        int64  `json:"index"`
	Key          string `json:"key"`
	RefSource    string `json:"ref_source"`
	ReleaseIdent string `json:"release_ident"`
	ReleaseYear  int64  `json:"release_year"`
	WorkIdent    string `json:"work_ident"`
}

// Release document.
type Release struct {
	ContainerName string `json:"container_name"`
	Contribs      []struct {
		Index   int64  `json:"index"`
		RawName string `json:"raw_name"`
		Role    string `json:"role"`
	} `json:"contribs"`
	ExtIDs struct {
		DOI         string `json:"doi"`
		PMID        string `json:"pmid"`
		PMCID       string `json:"pmcid"`
		Arxiv       string `json:"arxiv"`
		Core        string `json:"core"`
		WikidataQID string `json:"wikidata_qid"`
		Jstor       string `json:"jstor"`
	} `json:"ext_ids"`
	Ident       string `json:"ident"`
	Publisher   string `json:"publisher"`
	ReleaseDate string `json:"release_date"`
	ReleaseYear string `json:"release_year"`
	Issue       string `json:"issue"`
	Volume      string `json:"volume"`
	Pages       string `json:"pages"`
	Title       string `json:"title"`
	WorkID      string `json:"work_id"`
}
