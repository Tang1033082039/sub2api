//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type updateServiceCacheStub struct {
	data string
}

func (s *updateServiceCacheStub) GetUpdateInfo(context.Context) (string, error) {
	if s.data == "" {
		return "", errors.New("cache miss")
	}
	return s.data, nil
}

func (s *updateServiceCacheStub) SetUpdateInfo(_ context.Context, data string, _ time.Duration) error {
	s.data = data
	return nil
}

type updateServiceGitHubClientStub struct {
	release *GitHubRelease
	repo    string
}

func (s *updateServiceGitHubClientStub) FetchLatestRelease(_ context.Context, repo string) (*GitHubRelease, error) {
	s.repo = repo
	if s.release != nil {
		return s.release, nil
	}
	return &GitHubRelease{
		TagName: "v1.0.0",
		Name:    "Release 1.0.0",
	}, nil
}

func (s *updateServiceGitHubClientStub) DownloadFile(context.Context, string, string, int64) error {
	panic("DownloadFile should not be called")
}

func (s *updateServiceGitHubClientStub) FetchChecksumFile(context.Context, string) ([]byte, error) {
	panic("FetchChecksumFile should not be called")
}

func TestUpdateServiceUsesConfiguredRepo(t *testing.T) {
	client := &updateServiceGitHubClientStub{}
	svc := NewUpdateService(&updateServiceCacheStub{}, client, "0.9.0", "release", "owner/custom-repo")

	_, err := svc.fetchLatestRelease(context.Background())

	require.NoError(t, err)
	require.Equal(t, "owner/custom-repo", client.repo)
}

func TestUpdateServiceFallsBackToDefaultRepo(t *testing.T) {
	client := &updateServiceGitHubClientStub{}
	svc := NewUpdateService(&updateServiceCacheStub{}, client, "0.9.0", "release", "  ")

	_, err := svc.fetchLatestRelease(context.Background())

	require.NoError(t, err)
	require.Equal(t, defaultGitHubRepo, client.repo)
}

func TestUpdateServicePerformUpdateNoUpdateReturnsSentinel(t *testing.T) {
	svc := NewUpdateService(
		&updateServiceCacheStub{},
		&updateServiceGitHubClientStub{
			release: &GitHubRelease{
				TagName: "v0.1.132",
				Name:    "v0.1.132",
			},
		},
		"0.1.132",
		"release",
		"",
	)

	err := svc.PerformUpdate(context.Background())

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNoUpdateAvailable))
	require.ErrorIs(t, err, ErrNoUpdateAvailable)
}
