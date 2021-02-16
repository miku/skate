// Experimental: Turn the minimal cluster result (key, target, source) into an
// indexable biblio ref (10eb30251f89806cb7a0f147f427c5ea7e5f9941).
//
// Supports multiple input styles transparently, for the moment.
//
// "id style"
//
// ---- id, title, ... --- ---- target -------------- ---- source --------------
//
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae icg7bkoeqvfqnc5t5ot4evto6a
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae ichuaiowbvbx5ajae5ing27lka
// 10.1001/2012.jama.11164 zhscs2mjlvcdte2i3j44ibgzae io6b76ow6ngxnilc24qsf5kw6i
//
// "verify style"
//
// ---- target ------------------------------------------ ---- source ------------------------------------------ -- match ---- ---- match reason ------------------
//
// https://fatcat.wiki/release/a6xucdggk5h7bcmbxidvqt7hxe https://fatcat.wiki/release/amnpvj5ma5dxlc2a3x2bm2zbnq Status.STRONG Reason.SLUG_TITLE_AUTHOR_MATCH
// https://fatcat.wiki/release/vyppsuuh2bhapdwcqzln5momta https://fatcat.wiki/release/6gd53yl5yzakrlr72xeojamchi Status.DIFFERENT Reason.CONTRIB_INTERSECTION_EMPTY
// https://fatcat.wiki/release/hazousx6wna5bn5e27s5mrljzq https://fatcat.wiki/release/iajt2xam5nbc3ichkxxuhqaqw4 Status.DIFFERENT Reason.YEAR
//
// Input might change, so we keep this short.
package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/miku/skate"
	"github.com/miku/skate/parallel"
)

var (
	numWorkers = flag.Int("w", runtime.NumCPU(), "number of workers")
	batchSize  = flag.Int("b", 100000, "batch size")

	json         = jsoniter.ConfigCompatibleWithStandardLibrary
	bytesNewline = []byte("\n")
)

func main() {
	flag.Parse()
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var (
			fields                                                    = strings.Fields(string(p))
			target, source, matchStatus, matchReason, matchProvenance string
		)
		switch len(fields) {
		case 3:
			// Some join output.
			source = fields[2]
			target = fields[1]
			matchProvenance = "join"
		case 4:
			// Result of a "fuzzycat verify" run.
			source = strings.ReplaceAll(fields[1], "https://fatcat.wiki/release/", "")
			target = strings.ReplaceAll(fields[0], "https://fatcat.wiki/release/", "")
			matchProvenance = "fuzzycat/ebee2de"
			matchStatus = fields[2]
			matchReason = fields[3]
		}
		br := skate.BiblioRef{
			SourceReleaseIdent: source,
			TargetReleaseIdent: target,
			MatchStatus:        matchStatus,
			MatchReason:        matchReason,
			MatchProvenance:    matchProvenance,
		}
		b, err := json.Marshal(br)
		b = append(b, bytesNewline...)
		return b, err
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = *batchSize
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}
