package domain

type OAuthIdentity struct {
	Provider      string
	Subject       string
	Email         string
	EmailVerified bool
	FirstName     string
	LastName      string
}

type OAuthState struct {
	State         string
	Provider      string
	RedirectURI   string
	CodeChallenge string
}
