package s3

import (
	"testing"

	"github.com/containerops/dockyard/utils/setting"
)

func Test_amazons3save(t *testing.T) {
	var err error
	var url string

	if err = setting.SetConfig("../../../conf/containerops.conf"); err != nil {
		t.Error(err)
	}

	file := "amazons3_test.go"
	s := new(s3desc)
	url, err = s.Save(file)
	if err != nil {
		t.Error(err)
	}
	t.Log(url)
}
