package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	usecase "tenant_service/pkg/app"
	"tenant_service/pkg/domain"
)

const (
	googleOAuthProvider  = "google"
	discordOAuthProvider = "discord"
)

type OAuthProviderConfig struct {
	ClientID     string
	ClientSecret string
}

type oauthProviderClient struct {
	name         string
	client       *http.Client
	clientID     string
	clientSecret string
	authURL      string
	tokenURL     string
	userInfoURL  string
	scopes       []string
}

func NewOAuthProviderClients(client *http.Client, googleConfig, discordConfig OAuthProviderConfig) map[string]usecase.OAuthProviderClient {
	providers := map[string]usecase.OAuthProviderClient{}

	if strings.TrimSpace(googleConfig.ClientID) != "" && strings.TrimSpace(googleConfig.ClientSecret) != "" {
		providers[googleOAuthProvider] = &oauthProviderClient{
			name:         googleOAuthProvider,
			client:       client,
			clientID:     googleConfig.ClientID,
			clientSecret: googleConfig.ClientSecret,
			authURL:      "https://accounts.google.com/o/oauth2/v2/auth",
			tokenURL:     "https://oauth2.googleapis.com/token",
			userInfoURL:  "https://openidconnect.googleapis.com/v1/userinfo",
			scopes:       []string{"openid", "email", "profile"},
		}
	}

	if strings.TrimSpace(discordConfig.ClientID) != "" && strings.TrimSpace(discordConfig.ClientSecret) != "" {
		providers[discordOAuthProvider] = &oauthProviderClient{
			name:         discordOAuthProvider,
			client:       client,
			clientID:     discordConfig.ClientID,
			clientSecret: discordConfig.ClientSecret,
			authURL:      "https://discord.com/oauth2/authorize",
			tokenURL:     "https://discord.com/api/oauth2/token",
			userInfoURL:  "https://discord.com/api/users/@me",
			scopes:       []string{"identify", "email"},
		}
	}

	return providers
}

func (p *oauthProviderClient) AuthorizationURL(state, redirectURI, codeChallenge string) (string, error) {
	values := url.Values{}
	values.Set("client_id", p.clientID)
	values.Set("redirect_uri", redirectURI)
	values.Set("response_type", "code")
	values.Set("state", state)
	values.Set("scope", strings.Join(p.scopes, " "))
	values.Set("code_challenge", codeChallenge)
	values.Set("code_challenge_method", "S256")

	return p.authURL + "?" + values.Encode(), nil
}

func (p *oauthProviderClient) BigHillCode(ctx context.Context, code, redirectURI, codeVerifier string) (*domain.OAuthIdentity, error) {
	accessToken, err := p.bighillAccessToken(ctx, code, redirectURI, codeVerifier)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.userInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create oauth userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch oauth userinfo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oauth userinfo failed with status %d", resp.StatusCode)
	}

	switch p.name {
	case googleOAuthProvider:
		var payload struct {
			Sub           string `json:"sub"`
			Email         string `json:"email"`
			EmailVerified bool   `json:"email_verified"`
			GivenName     string `json:"given_name"`
			FamilyName    string `json:"family_name"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("failed to decode google userinfo: %w", err)
		}
		return &domain.OAuthIdentity{
			Provider:      googleOAuthProvider,
			Subject:       payload.Sub,
			Email:         strings.ToLower(strings.TrimSpace(payload.Email)),
			EmailVerified: payload.EmailVerified,
			FirstName:     strings.TrimSpace(payload.GivenName),
			LastName:      strings.TrimSpace(payload.FamilyName),
		}, nil
	case discordOAuthProvider:
		var payload struct {
			ID         string `json:"id"`
			Email      string `json:"email"`
			Verified   bool   `json:"verified"`
			Username   string `json:"username"`
			GlobalName string `json:"global_name"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("failed to decode discord userinfo: %w", err)
		}
		firstName := strings.TrimSpace(payload.GlobalName)
		if firstName == "" {
			firstName = strings.TrimSpace(payload.Username)
		}
		return &domain.OAuthIdentity{
			Provider:      discordOAuthProvider,
			Subject:       payload.ID,
			Email:         strings.ToLower(strings.TrimSpace(payload.Email)),
			EmailVerified: payload.Verified,
			FirstName:     firstName,
		}, nil
	default:
		return nil, usecase.ErrUnsupportedOAuthProvider
	}
}

func (p *oauthProviderClient) bighillAccessToken(ctx context.Context, code, redirectURI, codeVerifier string) (string, error) {
	form := url.Values{}
	form.Set("client_id", p.clientID)
	form.Set("client_secret", p.clientSecret)
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create oauth token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to bighill oauth code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
		return "", usecase.ErrInvalidOAuthCode
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("oauth token bighill failed with status %d", resp.StatusCode)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("failed to decode oauth token response: %w", err)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", usecase.ErrInvalidOAuthCode
	}

	return payload.AccessToken, nil
}
