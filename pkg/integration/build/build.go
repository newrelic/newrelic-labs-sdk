package build

type BuildInfo struct {
	Version   			string
	Commit 				string
	Date 				string
}

var (
	gBuildVersion 		string
	gBuildCommit        string
	gBuildDate          string
	gBuildInfo			BuildInfo
)

func init() {
	gBuildInfo.Version = gBuildVersion
	gBuildInfo.Commit = gBuildCommit
	gBuildInfo.Date = gBuildDate
}

func GetBuildInfo() BuildInfo {
	return gBuildInfo
}
