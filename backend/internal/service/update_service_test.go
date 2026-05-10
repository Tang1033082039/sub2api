package service

import (
	"context"
	"testing"
	"time"
)

type updateServiceTestCache struct{}

func (updateServiceTestCache) GetUpdateInfo(context.Context) (string, error) {
	return "", nil
}

func (updateServiceTestCache) SetUpdateInfo(context.Context, string, time.Duration) error {
	return nil
}

type updateServiceTestGitHubClient struct {
	repo string
}

func (c *updateServiceTestGitHubClient) FetchLatestRelease(_ context.Context, repo string) (*GitHubRelease, error) {
	c.repo = repo
	return &GitHubRelease{
		TagName: "v1.0.0",
		Name:    "Release 1.0.0",
	}, nil
}

func (*updateServiceTestGitHubClient) DownloadFile(context.Context, string, string, int64) error {
	return nil
}

func (*updateServiceTestGitHubClient) FetchChecksumFile(context.Context, string) ([]byte, error) {
	return nil, nil
}

func TestUpdateServiceUsesConfiguredRepo(t *testing.T) {
	client := &updateServiceTestGitHubClient{}
	svc := NewUpdateService(updateServiceTestCache{}, client, "0.9.0", "release", "owner/custom-repo")

	_, err := svc.fetchLatestRelease(context.Background())
	if err != nil {
		t.Fatalf("fetchLatestRelease() error = %v", err)
	}
	if client.repo != "owner/custom-repo" {
		t.Fatalf("FetchLatestRelease repo = %q, want %q", client.repo, "owner/custom-repo")
	}
}

func TestUpdateServiceFallsBackToDefaultRepo(t *testing.T) {
	client := &updateServiceTestGitHubClient{}
	svc := NewUpdateService(updateServiceTestCache{}, client, "0.9.0", "release", "  ")

	_, err := svc.fetchLatestRelease(context.Background())
	if err != nil {
		t.Fatalf("fetchLatestRelease() error = %v", err)
	}
	if client.repo != defaultGitHubRepo {
		t.Fatalf("FetchLatestRelease repo = %q, want %q", client.repo, defaultGitHubRepo)
	}
}
