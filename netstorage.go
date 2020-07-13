// Package netstorage provides interfacing the Akamai Netstorage(File/Object Store) API http(s) call
package netstorage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

// Netstorage struct provides all the necessary fields to
// create authorization headers.
// They are on the Akamai Netstorage account page.
// Hostname format should be "-nsu.akamaihd.net" and
// Note that don't expose Key on public repository.
// "Ssl" element is decided by "NetNetstorage" function - string "s" means https and "" does http.
type Netstorage struct {
	Hostname string
	Keyname  string
	Key      string
	Ssl      string
	Client   *http.Client
}

// NewNetstorage func creates and initiates Netstorage struct.
// ssl parameter decides https(true) and http(false) which means "s" and "".
func NewNetstorage(hostname, keyname, key string, ssl bool) *Netstorage {
	if hostname == "" || keyname == "" || key == "" {
		panic("[NetstorageError] You should input netstorage hostname, keyname and key all")
	}
	s := ""
	if ssl {
		s = "s"
	}
	return &Netstorage{hostname, keyname, key, s, http.DefaultClient}
}

// Only for upload action. (Used by _request func)
func _ifUploadAction(kwargs map[string]interface{}) (io.Reader, error) {
	var data io.Reader
	if kwargs["action"].(string) == "upload" {
		bArr, err := ioutil.ReadFile(kwargs["source"].(string))
		if err != nil {
			return nil, err
		}

		data = bytes.NewReader(bArr)
	}
	return data, nil
}

// Reads http body from response, closes response.Body and
// returns that string. (Used by _request func)
func _getBody(kwargs map[string]interface{}, response *http.Response) (string, error) {
	var body []byte
	var err error
	if kwargs["action"].(string) == "download" && response.StatusCode == 200 {
		localDestination := kwargs["destination"].(string)

		if localDestination == "" {
			localDestination = path.Base(kwargs["path"].(string))
		} else if s, err := os.Stat(localDestination); err == nil && s.IsDir() {
			localDestination = path.Join(localDestination, path.Base(kwargs["path"].(string)))
		}

		out, err := os.Create(localDestination)
		if err != nil {
			return "", err
		}
		defer out.Close()

		if _, err := io.Copy(out, response.Body); err != nil {
			return "", err
		}
		body = []byte("Download done")
	} else {
		body, err = ioutil.ReadAll(response.Body)
		if err != nil {
			return "", err
		}
	}

	return string(body), nil
}

// Create the authorization headers with Netstorage struct values then
// request to the Netstorage hostname, and return the response,
// the body string and the error.
func (ns *Netstorage) _request(kwargs map[string]interface{}) (*http.Response, string, error) {
	var err error

	nsPath := kwargs["path"].(string)
	if u, err := url.Parse(nsPath); strings.HasPrefix(nsPath, "/") && err == nil {
		nsPath = u.RequestURI()
	} else {
		return nil, "", fmt.Errorf("[Netstorage Error] Invalid netstorage path: %s", nsPath)
	}

	acsAction := fmt.Sprintf("version=1&action=%s", kwargs["action"].(string))
	acsAuthData := fmt.Sprintf("5, 0.0.0.0, 0.0.0.0, %d, %d, %s",
		time.Now().Unix(),
		rand.Intn(100000),
		ns.Keyname)

	signString := fmt.Sprintf("%s\nx-akamai-acs-action:%s\n", nsPath, acsAction)
	mac := hmac.New(sha256.New, []byte(ns.Key))
	mac.Write([]byte(acsAuthData + signString))
	acsAuthSign := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	var data io.Reader
	if _, exists := kwargs["content"]; exists {
		data = kwargs["content"].(io.Reader)
	} else {
		data, err = _ifUploadAction(kwargs)
		if err != nil {
			return nil, "", err
		}
	}

	method := kwargs["method"].(string)
	url := fmt.Sprintf("http%s://%s%s", ns.Ssl, ns.Hostname, nsPath)
	var request *http.Request
	if ctx, ok := kwargs["ctx"].(context.Context); ok {
		request, err = http.NewRequestWithContext(ctx, method, url, data)
	} else {
		request, err = http.NewRequest(method, url, data)
	}

	if err != nil {
		return nil, "", err
	}

	request.Header.Add("X-Akamai-ACS-Action", acsAction)
	request.Header.Add("X-Akamai-ACS-Auth-Data", acsAuthData)
	request.Header.Add("X-Akamai-ACS-Auth-Sign", acsAuthSign)
	request.Header.Add("Accept-Encoding", "identity")
	request.Header.Add("User-Agent", "NetStorageKit-Golang")

	response, err := ns.Client.Do(request)

	if err != nil {
		return nil, "", err
	}

	defer response.Body.Close()
	body, err := _getBody(kwargs, response)

	return response, body, err
}

// Dir returns the directory structure
func (ns *Netstorage) Dir(nsPath string) (*http.Response, string, error) {
	return ns.DirWithContext(context.Background(), nsPath)
}

// DirWithContext provides Dir behavior with context
func (ns *Netstorage) DirWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "dir&format=xml",
		"method": "GET",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Download returns the string "Download done" when the download completes.
// The first parameter is Netstorage source path and
// the second is Local destination path. If you put only the first parameter,
// it downloads to current local path with the first parameter's file name.
// From the third parameters will be ignored.
// Note that you can download only a file, not a directory.
func (ns *Netstorage) Download(path ...string) (*http.Response, string, error) {
	return ns.DownloadWithContext(context.Background(), path...)
}

// DownloadWithContext provides Download behavior with context
func (ns *Netstorage) DownloadWithContext(ctx context.Context, path ...string) (*http.Response, string, error) {
	nsSource := path[0]
	if strings.HasSuffix(nsSource, "/") {
		return nil, "", fmt.Errorf("[NetstorageError] Nestorage download path shouldn't be a directory: %s", nsSource)
	}

	localDestination := ""
	if len(path) >= 2 {
		localDestination = path[1]
	}

	return ns._request(map[string]interface{}{
		"action":      "download",
		"method":      "GET",
		"path":        nsSource,
		"destination": localDestination,
		"ctx":         ctx,
	})
}

// Du returns the disk usage information for a directory
func (ns *Netstorage) Du(nsPath string) (*http.Response, string, error) {
	return ns.DuWithContext(context.Background(), nsPath)
}

// DuWithContext adds Du behavior with context
func (ns *Netstorage) DuWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "du&format=xml",
		"method": "GET",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Stat returns the information about an object structure
func (ns *Netstorage) Stat(nsPath string) (*http.Response, string, error) {
	return ns.StatWithContext(context.Background(), nsPath)
}

// StatWithContext provides Stat behavior with context
func (ns *Netstorage) StatWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "stat&format=xml",
		"method": "GET",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Mkdir creates an empty directory
func (ns *Netstorage) Mkdir(nsPath string) (*http.Response, string, error) {
	return ns.MkdirWithContext(context.Background(), nsPath)
}

// MkdirWithContext provides Mkdir behavior with context
func (ns *Netstorage) MkdirWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "mkdir",
		"method": "POST",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Rmdir deletes an empty directory
func (ns *Netstorage) Rmdir(nsPath string) (*http.Response, string, error) {
	return ns.RmdirWithContext(context.Background(), nsPath)
}

// RmdirWithContext provides Rmdir behavior with context
func (ns *Netstorage) RmdirWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "rmdir",
		"method": "POST",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Mtime changes a file’s mtime
func (ns *Netstorage) Mtime(nsPath string, mtime int64) (*http.Response, string, error) {
	return ns.MtimeWithContext(context.Background(), nsPath, mtime)
}

// MtimeWithContext provides Mtime behavior with context
func (ns *Netstorage) MtimeWithContext(ctx context.Context, nsPath string, mtime int64) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": fmt.Sprintf("mtime&format=xml&mtime=%d", mtime),
		"method": "POST",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// Delete deletes an object/symbolic link
func (ns *Netstorage) Delete(nsPath string) (*http.Response, string, error) {
	return ns.DeleteWithContext(context.Background(), nsPath)
}

// DeleteWithContext provides Delete behavior with context
func (ns *Netstorage) DeleteWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "delete",
		"method": "POST",
		"path":   nsPath,
		"ctx":    ctx,
	})
}

// QuickDelete deletes a directory (i.e., recursively delete a directory tree)
// In order to use this func, you need to the privilege on the CP Code.
func (ns *Netstorage) QuickDelete(nsPath string) (*http.Response, string, error) {
	return ns.QuickDeleteWithContext(context.Background(), nsPath)
}

// QuickDeleteWithContext provides QuickDelete behavior with context.
func (ns *Netstorage) QuickDeleteWithContext(ctx context.Context, nsPath string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "quick-delete&quick-delete=imreallyreallysure",
		"method": "POST",
		"ctx":    ctx,
		"path":   nsPath,
	})
}

// Rename renames a file or symbolic link.
func (ns *Netstorage) Rename(nsTarget, nsDestination string) (*http.Response, string, error) {
	return ns.RenameWithContext(context.Background(), nsTarget, nsDestination)
}

// RenameWithContext provides Rename behavior with context.
func (ns *Netstorage) RenameWithContext(ctx context.Context, nsTarget, nsDestination string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "rename&destination=" + url.QueryEscape(nsDestination),
		"method": "POST",
		"ctx":    ctx,
		"path":   nsTarget,
	})
}

// Symlink creates a symbolic link.
func (ns *Netstorage) Symlink(nsTarget, nsDestination string) (*http.Response, string, error) {
	return ns.SymlinkWithContext(context.Background(), nsTarget, nsDestination)
}

// SymlinkWithContext adds SymLink behavior with context.
func (ns *Netstorage) SymlinkWithContext(ctx context.Context, nsTarget, nsDestination string) (*http.Response, string, error) {
	return ns._request(map[string]interface{}{
		"action": "symlink&target=" + url.QueryEscape(nsTarget),
		"method": "POST",
		"ctx":    ctx,
		"path":   nsDestination,
	})
}

// Upload uploads an object.
// The first parameter is the local source path and the second is
// the Netstorage destination path.
// If you put the directory path on "nsDestination" parameter, that filename
// will be the "localSource" parameter filename.
// Note that you can upload only a file, not a directory.
func (ns *Netstorage) Upload(localSource, nsDestination string) (*http.Response, string, error) {
	return ns.UploadWithContext(context.Background(), localSource, nsDestination)
}

// UploadWithContext adds Upload behavior with context.
func (ns *Netstorage) UploadWithContext(ctx context.Context, localSource, nsDestination string) (*http.Response, string, error) {
	s, err := os.Stat(localSource)

	if err != nil {
		return nil, "", err
	}

	if s.Mode().IsRegular() {
		if strings.HasSuffix(nsDestination, "/") {
			nsDestination = nsDestination + path.Base(localSource)
		}
	} else {
		return nil, "", fmt.Errorf("[NetstorageError] You should upload a file, not %s", localSource)
	}

	return ns._request(map[string]interface{}{
		"action": "upload",
		"method": "PUT",
		"source": localSource,
		"ctx":    ctx,
		"path":   nsDestination,
	})
}

// UploadContent uploads an object directly with its content.
func (ns *Netstorage) UploadContent(reader io.Reader, nsDestination string) (*http.Response, string, error) {
	return ns.UploadContentWithContext(context.Background(), reader, nsDestination)
}

// UploadContentWithContext adds UploadContent behavior with context.
func (ns *Netstorage) UploadContentWithContext(ctx context.Context, reader io.Reader, nsDestination string) (*http.Response, string, error) {
	if strings.HasSuffix(nsDestination, "/") {
		return nil, "", fmt.Errorf("[NetstorageError] Destination path should not be a directory")
	}

	return ns._request(map[string]interface{}{
		"action":  "upload",
		"method":  "PUT",
		"content": reader,
		"ctx":     ctx,
		"path":    nsDestination,
	})
}
