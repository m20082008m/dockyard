//adapt to docker API
package module

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/gorilla/mux"

	"github.com/containerops/dockyard/backend"
	"github.com/containerops/dockyard/models"
	"github.com/containerops/dockyard/utils"
)

var (
	DIGEST_INVALID        = "DIGEST_INVALID"
	NAME_INVALID          = "NAME_INVALID"
	TAG_INVALID           = "TAG_INVALID"
	NAME_UNKNOWN          = "NAME_UNKNOWN"
	MANIFEST_UNKNOWN      = "MANIFEST_UNKNOWN"
	MANIFEST_INVALID      = "MANIFEST_INVALID"
	MANIFEST_UNVERIFIED   = "MANIFEST_UNVERIFIED"
	MANIFEST_BLOB_UNKNOWN = "MANIFEST_BLOB_UNKNOWN"
	BLOB_UNKNOWN          = "BLOB_UNKNOWN"
	BLOB_UPLOAD_UNKNOWN   = "BLOB_UPLOAD_UNKNOWN"
	BLOB_UPLOAD_INVALID   = "BLOB_UPLOAD_INVALID"
)

type Errors struct {
	Errors []Error `json:"errors"`
}

type Error struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Detail  interface{} `json:"detail,omitempty"`
}

func FormatErr(code, message string, detail interface{}) ([]byte, error) {
	var errs = Errors{}

	item := Error{
		Code:    code,
		Message: message,
		Detail:  detail,
	}

	errs.Errors = append(errs.Errors, item)

	result, err := json.Marshal(errs)
	return result, err
}

func ParseManifest(data []byte, namespace, repository, tag string) (error, int64) {
	var manifest map[string]interface{}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err, 0
	}

	schemaVersion := int64(manifest["schemaVersion"].(float64))
	if schemaVersion == 1 {
		for k := len(manifest["history"].([]interface{})) - 1; k >= 0; k-- {
			v := manifest["history"].([]interface{})[k]
			compatibility := v.(map[string]interface{})["v1Compatibility"].(string)

			var image map[string]interface{}
			if err := json.Unmarshal([]byte(compatibility), &image); err != nil {
				return err, 0
			}

			i := map[string]string{}
			r := new(models.Repository)

			if k == 0 {
				i["Tag"] = tag
			}
			i["id"] = image["id"].(string)

			if err := r.PutJSONFromManifests(i, namespace, repository); err != nil {
				return err, 0
			}

			if k == 0 {
				if err := r.PutTagFromManifests(image["id"].(string), namespace, repository, tag, string(data), schemaVersion); err != nil {
					return err, 0
				}
			}
		}
	} else if schemaVersion == 2 {
		r := new(models.Repository)
		if err := r.PutTagFromManifests("schemaV2", namespace, repository, tag, string(data), schemaVersion); err != nil {
			return err, 0
		}
	} else {
		return fmt.Errorf("Invalid schema version"), 0
	}

	return nil, schemaVersion
}

//Upload the layer of image to object storage service,support to analyzed docker V1/V2 manifest now
func UploadLayer(data []byte) error {
	if backend.Drv == nil {
		return nil
	}

	var err error
	var manifest map[string]interface{}
	if err = json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	schemaVersion := int64(manifest["schemaVersion"].(float64))

	var layerdesc = []string{"", "fsLayers", "layers"}
	var tarsumdesc = []string{"", "blobSum", "digest"}
	section := layerdesc[schemaVersion]
	item := tarsumdesc[schemaVersion]

	var tarsumlist []string
	var pathlist []string
	var issuccess bool = true
	for j := len(manifest[section].([]interface{})) - 1; j >= 0; j-- {
		blobsum := manifest[section].([]interface{})[j].(map[string]interface{})[item].(string)
		tarsum := strings.Split(blobsum, ":")[1]

		i := new(models.Image)
		var exists bool
		if exists, err = i.Get(tarsum); err != nil {
			return err
		} else if !exists {
			return fmt.Errorf("layer is not existent")
		}

		if _, err = os.Stat(i.Path); err != nil {
			continue
		}

		tarsumlist = append(tarsumlist, tarsum)
		pathlist = append(pathlist, i.Path)
		//TODO: 1)save same layer mutiple times 2)need to solve upload failed situation
		if _, err = backend.Drv.Save(i.Path); err != nil {
			issuccess = false
			break
		}
	}

	//Remove the layer in oss while upload failed
	if !issuccess {
		for _, v := range pathlist {
			backend.Drv.Delete(v)
		}

		return err
	}

	return nil
}

func DownloadLayer(layerpath string) ([]byte, error) {
	var content []byte
	var err error

	content, err = ioutil.ReadFile(layerpath)
	if err != nil {
		if backend.Drv == nil {
			return []byte(""), fmt.Errorf("Read file failure: %v", err.Error())
		}

		content, err = backend.Drv.Get(layerpath)
		if err != nil {
			return []byte(""), fmt.Errorf("Failed to download layer: %v", err.Error())
		}
	}

	return content, nil
}

func SaveLayerLocal(srcPath, srcFile, dstPath, dstFile string, reqbody []byte) (int, error) {
	if !utils.IsDirExist(dstPath) {
		os.MkdirAll(dstPath, os.ModePerm)
	}

	if utils.IsFileExist(dstFile) {
		os.Remove(dstFile)
	}

	var data []byte
	if _, err := os.Stat(srcFile); err == nil {
		data, _ = ioutil.ReadFile(srcFile)
		if err := ioutil.WriteFile(dstFile, data, 0777); err != nil {
			return 0, err
		}
		os.RemoveAll(srcPath)
	} else {
		data = reqbody
		if err := ioutil.WriteFile(dstFile, data, 0777); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

//codes as below are ported to support for docker to parse request URL,and it would be update soon
func parseIP(ipStr string) net.IP {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		fmt.Errorf("Invalid remote IP address: %q", ipStr)
	}
	return ip
}

func RemoteAddr(r *http.Request) string {
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		proxies := strings.Split(prior, ",")
		if len(proxies) > 0 {
			remoteAddr := strings.Trim(proxies[0], " ")
			if parseIP(remoteAddr) != nil {
				return remoteAddr
			}
		}
	}

	if realIP := r.Header.Get("X-Real-Ip"); realIP != "" {
		if parseIP(realIP) != nil {
			return realIP
		}
	}

	return r.RemoteAddr
}

const (
	RouteNameBase            = "base"
	RouteNameBlob            = "blob"
	RouteNameManifest        = "manifest"
	RouteNameTags            = "tags"
	RouteNameBlobUpload      = "blob-upload"
	RouteNameBlobUploadChunk = "blob-upload-chunk"
)

type URLBuilder struct {
	root   *url.URL
	router *mux.Router
}

type RouteDescriptor struct {
	Name string
	Path string
}

var RepositoryNameComponentRegexp = regexp.MustCompile(`[a-z0-9]+(?:[._-][a-z0-9]+)*`)
var RepositoryNameRegexp = regexp.MustCompile(`(?:` + RepositoryNameComponentRegexp.String() + `/)*` + RepositoryNameComponentRegexp.String())
var TagNameRegexp = regexp.MustCompile(`[\w][\w.-]{0,127}`)
var DigestRegexp = regexp.MustCompile(`[a-zA-Z0-9-_+.]+:[a-fA-F0-9]+`)

var routeDescriptors = []RouteDescriptor{
	{
		Name: RouteNameBase,
		Path: "/v2/",
	},
	{
		Name: RouteNameBlob,
		Path: "/v2/{name:" + RepositoryNameRegexp.String() + "}/blobs/{digest:" + DigestRegexp.String() + "}",
	},
	{
		Name: RouteNameManifest,
		Path: "/v2/{name:" + RepositoryNameRegexp.String() + "}/manifests/{reference:" + TagNameRegexp.String() + "|" + DigestRegexp.String() + "}",
	},
	{
		Name: RouteNameTags,
		Path: "/v2/{name:" + RepositoryNameRegexp.String() + "}/tags/list",
	},
	{
		Name: RouteNameBlobUpload,
		Path: "/v2/{name:" + RepositoryNameRegexp.String() + "}/blobs/uploads/",
	},
	{
		Name: RouteNameBlobUploadChunk,
		Path: "/v2/{name:" + RepositoryNameRegexp.String() + "}/blobs/uploads/{uuid:[a-zA-Z0-9-_.=]+}",
	},
}

func NewURLBuilderFromRequest(r *http.Request) *URLBuilder {
	var scheme string

	forwardedProto := r.Header.Get("X-Forwarded-Proto")
	switch {
	case len(forwardedProto) > 0:
		scheme = forwardedProto
	case r.TLS != nil:
		scheme = "https"
	case len(r.URL.Scheme) > 0:
		scheme = r.URL.Scheme
	default:
		scheme = "http"
	}

	host := r.Host
	forwardedHost := r.Header.Get("X-Forwarded-Host")
	if len(forwardedHost) > 0 {
		hosts := strings.SplitN(forwardedHost, ",", 2)
		host = strings.TrimSpace(hosts[0])
	}

	u := &url.URL{
		Scheme: scheme,
		Host:   host,
	}
	/*
		basePath := routeDescriptorsMap[RouteNameBase].Path
		requestPath := r.URL.Path
		index := strings.Index(requestPath, basePath)
		if index > 0 {
			u.Path = requestPath[0 : index+1]
		}
	*/
	return NewURLBuilder(u)
}

func Router() *mux.Router {
	return RouterWithPrefix("")
}

func RouterWithPrefix(prefix string) *mux.Router {
	rootRouter := mux.NewRouter()
	router := rootRouter
	if prefix != "" {
		router = router.PathPrefix(prefix).Subrouter()
	}

	router.StrictSlash(true)

	for _, descriptor := range routeDescriptors {
		router.Path(descriptor.Path).Name(descriptor.Name)
	}

	return rootRouter
}

func NewURLBuilder(root *url.URL) *URLBuilder {
	return &URLBuilder{
		root:   root,
		router: Router(),
	}
}

func (ub *URLBuilder) BuildBlobURL(name string, dgst string) (string, error) {
	route := ub.cloneRoute(RouteNameBlob)

	layerURL, err := route.URL("name", name, "digest", dgst)
	if err != nil {
		return "", err
	}

	return layerURL.String(), nil
}

func (ub *URLBuilder) BuildManifestURL(name, reference string) (string, error) {
	route := ub.cloneRoute(RouteNameManifest)

	manifestURL, err := route.URL("name", name, "reference", reference)
	if err != nil {
		return "", err
	}

	return manifestURL.String(), nil
}

func (ub *URLBuilder) cloneRoute(name string) clonedRoute {
	route := new(mux.Route)
	root := new(url.URL)

	*route = *ub.router.GetRoute(name)
	*root = *ub.root

	return clonedRoute{Route: route, root: root}
}

type clonedRoute struct {
	*mux.Route
	root *url.URL
}

func (cr clonedRoute) URL(pairs ...string) (*url.URL, error) {
	routeURL, err := cr.Route.URL(pairs...)
	if err != nil {
		return nil, err
	}

	if routeURL.Scheme == "" && routeURL.User == nil && routeURL.Host == "" {
		routeURL.Path = routeURL.Path[1:]
	}

	return cr.root.ResolveReference(routeURL), nil
}
