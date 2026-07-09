package rest

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"lib/shared_lib/transport"
	"net/mail"
	"reflect"
	"regexp"
	"strings"
	"sync"
	usecase "tenant_service/pkg/app"
	"tenant_service/pkg/domain"
	"time"
	"unicode"

	"encoding/json"

	"github.com/nyaruka/phonenumbers"

	log "github.com/sirupsen/logrus"

	validator "github.com/go-playground/validator/v10"
)

var postCodeRegexCache sync.Map // map[string]*regexp.Regexp

type profileDTO struct {
	ID           string `json:"id"`
	Email        string `json:"email" validate:"required,max=250,email_rfc5322"`
	FirstName    string `json:"firstName" validate:"required,sanatizeugc,min=2,max=100"`
	LastName     string `json:"lastName" validate:"required,sanatizeugc,min=2,max=100"`
	PhoneNumber  string `json:"phoneNumber" validate:"required,max=20,phone_by_cc"`
	DateOfBirth  string `json:"dateOfBirth" validate:"required,datetime=2006-01-02,dob"`
	CountryCode  string `json:"countryCode" validate:"required,len=2,uppercase,iso3166_1_alpha2"`
	AddressLine1 string `json:"addressLine1" validate:"required,sanatizeugc,min=2,max=100"`
	AddressLine2 string `json:"addressLine2,omitempty" validate:"sanatizeugc,max=100"`
	City         string `json:"city" validate:"required,sanatizeugc,min=2,max=50"`
	State        string `json:"state,omitempty" validate:"sanatizeugc,max=50"`
	PostalCode   string `json:"postalCode" validate:"required,sanatizeugc,min=2,max=20"`
	Country      string `json:"country" validate:"required,sanatizeugc,min=2,max=50"`
}

type passwordDTO struct {
	Password string `json:"password" validate:"required,min=8,max=100,pwd"`
}

type passwordValidationDTO struct {
	Email    string `json:"email" validate:"required,max=250,email_rfc5322"`
	Password string `json:"password" validate:"required,min=8,max=100,pwd"`
}

type passwordResultDTO struct {
	IsValid bool   `json:"isValid"`
	Token   string `json:"token,omitempty"`
}

type emailVerificationDTO struct {
	Token string `json:"token" validate:"required"`
}

type huggingFaceTokenDTO struct {
	Token string `json:"token" validate:"required"`
}

type oauthAuthorizeRequestDTO struct {
	RedirectURI   string `json:"redirectUri" validate:"required,url"`
	CodeChallenge string `json:"codeChallenge" validate:"required,min=43,max=128"`
}

type oauthAuthorizeResultDTO struct {
	AuthorizationURL string `json:"authorizationUrl"`
	State            string `json:"state"`
}

type oauthSessionRequestDTO struct {
	Code         string `json:"code" validate:"required"`
	State        string `json:"state" validate:"required"`
	RedirectURI  string `json:"redirectUri" validate:"required,url"`
	CodeVerifier string `json:"codeVerifier" validate:"required,min=43,max=128"`
}

type oauthSessionResultDTO struct {
	Verified  bool   `json:"verified"`
	Token     string `json:"token,omitempty"`
	Provider  string `json:"provider,omitempty"`
	IsNewUser bool   `json:"isNewUser,omitempty"`
}

type organizationDTO struct {
	OrgID       string                `json:"orgId"`
	DisplayName string                `json:"displayName"`
	Membership  organizationMemberDTO `json:"membership"`
}

type organizationMemberDTO struct {
	OrgID  string `json:"orgId,omitempty"`
	UserID string `json:"userId" validate:"required,uuid4"`
	Email  string `json:"email,omitempty"`
	Role   string `json:"role" validate:"required,oneof=consumer ml_researcher org_admin"`
	Status string `json:"status,omitempty" validate:"omitempty,oneof=active invited disabled"`
}

type organizationMembersDTO struct {
	Members []organizationMemberDTO `json:"members"`
}

type profileAccountDTO struct {
	ID          string `json:"id"` // ID is returned on create and passed in the header elsewhere
	Email       string `json:"email" validate:"required,max=250,email_rfc5322"`
	PhoneNumber string `json:"phoneNumber" validate:"required,max=20,phone_by_cc"`
	CountryCode string `json:"countryCode" validate:"required,len=2,uppercase,iso3166_1_alpha2"`
	Password    string `json:"password" validate:"required,min=8,max=100,pwd"`
}

type profilesDTOAdapter struct {
	validator *validator.Validate
}

type ProfilesDTOAdapter interface {
	ToDTO(ctx context.Context, profileModel *domain.Profile) ([]byte, error)
	FromDTO(ctx context.Context, profileBytes []byte) (*domain.Profile, error)
	FromPasswordDTO(ctx context.Context, passwordBytes []byte) (string, error)
	FromPasswordValidationDTO(ctx context.Context, passwordValidationBytes []byte) (string, string, error)
	FromEmailVerificationDTO(ctx context.Context, emailVerificationBytes []byte) (string, error)
	FromHuggingFaceTokenDTO(ctx context.Context, tokenBytes []byte) (string, error)
	FromProfileAccountDTO(ctx context.Context, profileAccountBytes []byte) (*domain.ProfileAccount, error)
	FromOAuthAuthorizeRequestDTO(ctx context.Context, requestBytes []byte) (*usecase.OAuthAuthorizeRequest, error)
	ToOAuthAuthorizeResultDTO(ctx context.Context, result *usecase.OAuthAuthorizeResult) ([]byte, error)
	FromOAuthSessionRequestDTO(ctx context.Context, requestBytes []byte) (*usecase.OAuthSessionRequest, error)
	ToOAuthSessionResultDTO(ctx context.Context, result *usecase.OAuthSessionResult) ([]byte, error)
	FromOrganizationMemberDTO(ctx context.Context, orgID uuid.UUID, requestBytes []byte) (*domain.OrganizationMembership, error)
	ToOrganizationDTO(ctx context.Context, organization *domain.Organization, membership *domain.OrganizationMembership) ([]byte, error)
	ToOrganizationMembersDTO(ctx context.Context, memberships []*domain.OrganizationMembership) ([]byte, error)
	ToProfileAccountDTO(ctx context.Context, profileAccount *domain.ProfileAccount) ([]byte, error)
	ToPasswordResultDTO(ctx context.Context, isValid bool, token string) ([]byte, error)
}

func (a *profilesDTOAdapter) FromHuggingFaceTokenDTO(ctx context.Context, tokenBytes []byte) (string, error) {
	log.Trace("profilesDTOAdapter FromHuggingFaceTokenDTO")

	var tokenDTO huggingFaceTokenDTO
	err := json.Unmarshal(tokenBytes, &tokenDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json unmarshalling hugging face token failed")
		return "", fmt.Errorf("validation error. json unmarshalling hugging face token failed: %w", err)
	}

	if err := a.validator.Struct(&tokenDTO); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate hugging face token")
		return "", fmt.Errorf("validation error. failed to validate hugging face token: %w", err)
	}

	return tokenDTO.Token, nil
}

func NewProfilesDTOAdapter() *profilesDTOAdapter {
	v := validator.New()
	dtoAdapter := &profilesDTOAdapter{
		validator: v,
	}
	_ = v.RegisterValidation("dob", dobValidator)
	_ = v.RegisterValidation("pwd", passwordValidator)
	_ = v.RegisterValidation("email_rfc5322", emailValidator)
	_ = v.RegisterValidation("phone_by_cc", phoneByCountryValidator)
	_ = v.RegisterValidation("sanatizeugc", SanatizeUGC)
	v.RegisterStructValidation(profileStructLevel, &profileDTO{})

	return dtoAdapter
}

func (a *profilesDTOAdapter) ToDTO(ctx context.Context, profileModel *domain.Profile) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToDTO")

	profileDTO := a.toDTO(profileModel)
	profileDTOBytes, err := json.Marshal(profileDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding profile failed")
		return nil, err
	}

	return profileDTOBytes, nil
}

func (a *profilesDTOAdapter) ToProfileAccountDTO(ctx context.Context, profileAccount *domain.ProfileAccount) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToProfileAccountDTO")

	profileAccountDTO := profileAccountDTO{
		ID:          profileAccount.ID.String(),
		Email:       profileAccount.Email,
		PhoneNumber: profileAccount.PhoneNumber,
		CountryCode: profileAccount.CountryCode,
	}

	profileAccountDTOBytes, err := json.Marshal(profileAccountDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding profile account failed")
		return nil, err
	}

	return profileAccountDTOBytes, nil
}

func (a *profilesDTOAdapter) FromDTO(ctx context.Context, profileBytes []byte) (*domain.Profile, error) {
	log.Trace("profilesDTOAdapter FromDTO")

	var profileModel *domain.Profile
	var profileDTO profileDTO
	err := json.Unmarshal(profileBytes, &profileDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("profile_bytes", string(profileBytes)).Error("json unmarshalling profile failed")
		return nil, fmt.Errorf("validation error. json unmarshalling profile failed: %w", err)
	}

	profileModel, err = a.fromDTO(ctx, &profileDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed convert from profileDTO to domain object")
		return nil, fmt.Errorf("validation error. converting from profileDTO to domain object failed: %w", err)
	}

	return profileModel, nil
}

func (a *profilesDTOAdapter) FromPasswordDTO(ctx context.Context, passwordBytes []byte) (string, error) {
	log.Trace("profilesDTOAdapter FromPasswordDTO")

	var passwordDTO passwordDTO
	err := json.Unmarshal(passwordBytes, &passwordDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("password_bytes", string(passwordBytes)).Error("json unmarshalling password failed")
		return "", fmt.Errorf("validation error. json unmarshalling password failed: %w", err)
	}

	if err := a.validator.Struct(&passwordDTO); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate password")
		return "", fmt.Errorf("validation error. failed to validate password: %w", err)
	}

	return passwordDTO.Password, nil
}

func (a *profilesDTOAdapter) FromPasswordValidationDTO(ctx context.Context, passwordValidationBytes []byte) (string, string, error) {
	log.Trace("profilesDTOAdapter FromPasswordValidationDTO")

	var passwordValidationDTO passwordValidationDTO
	err := json.Unmarshal(passwordValidationBytes, &passwordValidationDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("password_validation_bytes", string(passwordValidationBytes)).Error("json unmarshalling password validation failed")
		return "", "", fmt.Errorf("validation error. json unmarshalling password validation failed: %w", err)
	}

	if err := a.validator.Struct(&passwordValidationDTO); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate password validation")
		return "", "", fmt.Errorf("validation error. failed to validate password validation: %w", err)
	}

	return passwordValidationDTO.Email, passwordValidationDTO.Password, nil
}

func (a *profilesDTOAdapter) ToPasswordResultDTO(ctx context.Context, isValid bool, token string) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToPasswordResultDTO")

	result := passwordResultDTO{
		IsValid: isValid,
		Token:   token,
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding password result failed")
		return nil, err
	}

	return resultBytes, nil
}

func (a *profilesDTOAdapter) FromEmailVerificationDTO(ctx context.Context, emailVerificationBytes []byte) (string, error) {
	log.Trace("profilesDTOAdapter FromEmailVerificationDTO")

	var emailVerification emailVerificationDTO
	err := json.Unmarshal(emailVerificationBytes, &emailVerification)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("email_verification_bytes", string(emailVerificationBytes)).Error("json unmarshalling email verification failed")
		return "", fmt.Errorf("validation error. json unmarshalling email verification failed: %w", err)
	}

	if err := a.validator.Struct(&emailVerification); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate email verification")
		return "", fmt.Errorf("validation error. failed to validate email verification: %w", err)
	}

	return emailVerification.Token, nil
}

func (a *profilesDTOAdapter) FromOAuthAuthorizeRequestDTO(ctx context.Context, requestBytes []byte) (*usecase.OAuthAuthorizeRequest, error) {
	log.Trace("profilesDTOAdapter FromOAuthAuthorizeRequestDTO")

	var request oauthAuthorizeRequestDTO
	if err := json.Unmarshal(requestBytes, &request); err != nil {
		log.WithContext(ctx).WithError(err).Error("json unmarshalling oauth authorize request failed")
		return nil, fmt.Errorf("validation error. json unmarshalling oauth authorize request failed: %w", err)
	}
	if err := a.validator.Struct(&request); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate oauth authorize request")
		return nil, fmt.Errorf("validation error. failed to validate oauth authorize request: %w", err)
	}

	return &usecase.OAuthAuthorizeRequest{
		RedirectURI:   request.RedirectURI,
		CodeChallenge: request.CodeChallenge,
	}, nil
}

func (a *profilesDTOAdapter) ToOAuthAuthorizeResultDTO(ctx context.Context, result *usecase.OAuthAuthorizeResult) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToOAuthAuthorizeResultDTO")

	payload, err := json.Marshal(oauthAuthorizeResultDTO{
		AuthorizationURL: result.AuthorizationURL,
		State:            result.State,
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding oauth authorize result failed")
		return nil, err
	}
	return payload, nil
}

func (a *profilesDTOAdapter) FromOAuthSessionRequestDTO(ctx context.Context, requestBytes []byte) (*usecase.OAuthSessionRequest, error) {
	log.Trace("profilesDTOAdapter FromOAuthSessionRequestDTO")

	var request oauthSessionRequestDTO
	if err := json.Unmarshal(requestBytes, &request); err != nil {
		log.WithContext(ctx).WithError(err).Error("json unmarshalling oauth session request failed")
		return nil, fmt.Errorf("validation error. json unmarshalling oauth session request failed: %w", err)
	}
	if err := a.validator.Struct(&request); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate oauth session request")
		return nil, fmt.Errorf("validation error. failed to validate oauth session request: %w", err)
	}

	return &usecase.OAuthSessionRequest{
		Code:         request.Code,
		State:        request.State,
		RedirectURI:  request.RedirectURI,
		CodeVerifier: request.CodeVerifier,
	}, nil
}

func (a *profilesDTOAdapter) ToOAuthSessionResultDTO(ctx context.Context, result *usecase.OAuthSessionResult) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToOAuthSessionResultDTO")

	payload, err := json.Marshal(oauthSessionResultDTO{
		Verified:  result != nil && result.Token != "",
		Token:     result.Token,
		Provider:  result.Provider,
		IsNewUser: result.IsNewUser,
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding oauth session result failed")
		return nil, err
	}
	return payload, nil
}

func (a *profilesDTOAdapter) FromOrganizationMemberDTO(ctx context.Context, orgID uuid.UUID, requestBytes []byte) (*domain.OrganizationMembership, error) {
	log.Trace("profilesDTOAdapter FromOrganizationMemberDTO")

	var request organizationMemberDTO
	if err := json.Unmarshal(requestBytes, &request); err != nil {
		log.WithContext(ctx).WithError(err).Error("json unmarshalling organization member request failed")
		return nil, fmt.Errorf("validation error. json unmarshalling organization member request failed: %w", err)
	}
	if err := a.validator.Struct(&request); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate organization member request")
		return nil, fmt.Errorf("validation error. failed to validate organization member request: %w", err)
	}
	userID, err := uuid.Parse(request.UserID)
	if err != nil || userID == uuid.Nil {
		return nil, fmt.Errorf("validation error. invalid organization member user id")
	}
	return &domain.OrganizationMembership{
		OrgID:  orgID,
		UserID: userID,
		Role:   request.Role,
		Status: request.Status,
	}, nil
}

func (a *profilesDTOAdapter) ToOrganizationDTO(ctx context.Context, organization *domain.Organization, membership *domain.OrganizationMembership) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToOrganizationDTO")

	payload, err := json.Marshal(organizationDTO{
		OrgID:       organization.ID.String(),
		DisplayName: organization.DisplayName,
		Membership:  toOrganizationMemberDTO(membership),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding organization failed")
		return nil, err
	}
	return payload, nil
}

func (a *profilesDTOAdapter) ToOrganizationMembersDTO(ctx context.Context, memberships []*domain.OrganizationMembership) ([]byte, error) {
	log.Trace("profilesDTOAdapter ToOrganizationMembersDTO")

	memberDTOs := make([]organizationMemberDTO, 0, len(memberships))
	for _, membership := range memberships {
		memberDTOs = append(memberDTOs, toOrganizationMemberDTO(membership))
	}
	payload, err := json.Marshal(organizationMembersDTO{Members: memberDTOs})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("json encoding organization members failed")
		return nil, err
	}
	return payload, nil
}

func toOrganizationMemberDTO(membership *domain.OrganizationMembership) organizationMemberDTO {
	log.Trace("toOrganizationMemberDTO")

	if membership == nil {
		return organizationMemberDTO{}
	}
	return organizationMemberDTO{
		OrgID:  membership.OrgID.String(),
		UserID: membership.UserID.String(),
		Email:  membership.Email,
		Role:   membership.Role,
		Status: membership.Status,
	}
}

func (a *profilesDTOAdapter) FromProfileAccountDTO(ctx context.Context, profileAccountBytes []byte) (*domain.ProfileAccount, error) {
	log.Trace("profilesDTOAdapter FromProfileAccountDTO")

	var profileAccountDTO profileAccountDTO
	err := json.Unmarshal(profileAccountBytes, &profileAccountDTO)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("profile_account_bytes", string(profileAccountBytes)).Error("json unmarshalling profile account failed")
		return nil, fmt.Errorf("validation error. json unmarshalling profile account failed: %w", err)
	}

	if err := a.validator.Struct(&profileAccountDTO); err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to validate profile account")
		return nil, fmt.Errorf("validation error. failed to validate profile account: %w", err)
	}

	normalizedPhone, err := normalizePhoneNumber(profileAccountDTO.PhoneNumber, profileAccountDTO.CountryCode)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to normalize phone number for profile account")
		return nil, fmt.Errorf("validation error. failed to validate profile account: %w", err)
	}

	modelProfileAccount := domain.ProfileAccount{
		Email:       strings.ToLower(profileAccountDTO.Email),
		PhoneNumber: normalizedPhone,
		CountryCode: profileAccountDTO.CountryCode,
		Password:    profileAccountDTO.Password,
	}

	return &modelProfileAccount, nil
}

func (a *profilesDTOAdapter) fromDTO(ctx context.Context, profileDTO *profileDTO) (*domain.Profile, error) {
	log.Trace("profilesDTOAdapter fromDTO")

	if err := a.validator.Struct(profileDTO); err != nil {
		log.WithContext(ctx).WithError(err).Warnf("invalid DTO: %v", profileDTO)
		return nil, err
	}

	var dob time.Time
	var err error
	dob, err = time.Parse("2006-01-02", profileDTO.DateOfBirth)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("invalid date format for DateOfBirth")
		return nil, fmt.Errorf("validation error. invalid date format for Date Of Birth: %w", err)
	}

	normalizedPhone, err := normalizePhoneNumber(profileDTO.PhoneNumber, profileDTO.CountryCode)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("failed to normalize phone number for profile")
		return nil, fmt.Errorf("validation error. failed to validate profile phone number: %w", err)
	}

	modelProfile := domain.Profile{
		ProfileAccount: domain.ProfileAccount{
			Email:       strings.ToLower(profileDTO.Email),
			PhoneNumber: normalizedPhone,
			CountryCode: profileDTO.CountryCode,
		},
		FirstName:    profileDTO.FirstName,
		LastName:     profileDTO.LastName,
		DateOfBirth:  dob,
		AddressLine1: profileDTO.AddressLine1,
		AddressLine2: profileDTO.AddressLine2,
		City:         profileDTO.City,
		State:        profileDTO.State,
		PostalCode:   profileDTO.PostalCode,
		Country:      profileDTO.Country,
	}

	return &modelProfile, nil
}

func (a *profilesDTOAdapter) toDTO(profileModel *domain.Profile) *profileDTO {
	log.Trace("profilesDTOAdapter toDTO")

	dtoProfile := profileDTO{
		ID:           profileModel.ID.String(),
		Email:        profileModel.Email,
		FirstName:    profileModel.FirstName,
		LastName:     profileModel.LastName,
		PhoneNumber:  profileModel.PhoneNumber,
		DateOfBirth:  profileModel.DateOfBirth.Format("2006-01-02"),
		CountryCode:  profileModel.CountryCode,
		AddressLine1: profileModel.AddressLine1,
		AddressLine2: profileModel.AddressLine2,
		City:         profileModel.City,
		State:        profileModel.State,
		PostalCode:   profileModel.PostalCode,
		Country:      profileModel.Country,
	}
	return &dtoProfile
}

func passwordValidator(fl validator.FieldLevel) bool {
	log.Trace("profilesDTOAdapter passwordValidator")

	s := fl.Field().String()
	var hasLetter, hasDigit, hasSpecial bool

	for _, r := range s {
		switch {
		case unicode.IsLetter(r):
			hasLetter = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			hasSpecial = true
		default:
			// whitespace or other categories don't count toward special
		}
	}
	return hasLetter && hasDigit && hasSpecial
}

func phoneByCountryValidator(fl validator.FieldLevel) bool {
	log.Trace("profilesDTOAdapter phoneByCountryValidator")

	phone := strings.TrimSpace(fl.Field().String())
	if phone == "" { // handled by required tag
		return true
	}

	parent := fl.Parent()
	countryField := parent.FieldByName("CountryCode")
	if !countryField.IsValid() || countryField.Kind() != reflect.String {
		return false
	}

	countryCode := strings.ToUpper(strings.TrimSpace(countryField.String()))
	if countryCode == "" {
		return false
	}

	num, err := phonenumbers.Parse(phone, countryCode)
	if err != nil || !phonenumbers.IsValidNumber(num) {
		return false
	}

	formatted := phonenumbers.Format(num, phonenumbers.E164)
	return phone == formatted
}

func normalizePhoneNumber(phone, countryCode string) (string, error) {
	trimmedPhone := strings.TrimSpace(phone)
	trimmedCountry := strings.ToUpper(strings.TrimSpace(countryCode))

	if trimmedPhone == "" || trimmedCountry == "" {
		return "", fmt.Errorf("validation error. phone number and country code are required")
	}

	num, err := phonenumbers.Parse(trimmedPhone, trimmedCountry)
	if err != nil || !phonenumbers.IsValidNumber(num) {
		return "", fmt.Errorf("validation error. invalid phone number for country %s", trimmedCountry)
	}

	formatted := phonenumbers.Format(num, phonenumbers.E164)
	if formatted != trimmedPhone {
		return "", fmt.Errorf("validation error. phone number must be in E.164 format")
	}

	return formatted, nil
}

func dobValidator(fl validator.FieldLevel) bool {
	log.Trace("profilesDTOAdapter dobValidator")

	s := fl.Field().String()
	if s == "" {
		return true // handled by omitempty
	}
	dob, err := time.Parse("2006-01-02", s)
	if err != nil {
		return false
	}
	today := time.Now().UTC()
	if dob.After(today) {
		return false
	}

	age := today.Year() - dob.Year()
	if today.YearDay() < dob.YearDay() {
		age--
	}
	return age >= 18 && age <= 120
}

func emailValidator(fl validator.FieldLevel) bool {
	log.Trace("profilesDTOAdapter emailValidator")

	s := strings.TrimSpace(fl.Field().String())
	if s == "" {
		return false
	}

	addr, err := mail.ParseAddress(s)
	if err != nil {
		return false
	}

	if addr.Name != "" || addr.Address != s {
		return false
	}

	local, domain, ok := strings.Cut(addr.Address, "@")
	if !ok || local == "" || domain == "" {
		return false
	}

	if len(local) > 64 || len(addr.Address) > 254 {
		return false
	}

	if !strings.Contains(domain, ".") {
		return false
	}
	return true
}

func profileStructLevel(sl validator.StructLevel) {
	log.Trace("profilesDTOAdapter profileStructLevel")

	var dto profileDTO
	switch x := sl.Current().Interface().(type) {
	case profileDTO:
		dto = x
	case *profileDTO:
		if x == nil {
			return
		}
		dto = *x
	default:
		return
	}

	if dto.PostalCode != "" && dto.CountryCode != "" {
		if err := ValidatePostalCode(dto.CountryCode, dto.PostalCode); err != nil {
			sl.ReportError(dto.PostalCode, "PostalCode", "postalCode", "postcode_by_cc", "")
		}
	}
}

// compile-once-per-country
func getPostalRegex(countryCode string) (*regexp.Regexp, error) {
	log.Trace("profilesDTOAdapter getPostalRegex")

	cc := strings.ToUpper(strings.TrimSpace(countryCode))
	if cc == "" {
		return nil, errors.New("empty country code")
	}

	if v, ok := postCodeRegexCache.Load(cc); ok {
		return v.(*regexp.Regexp), nil
	}

	raw := rawPatterns()
	p, ok := raw[cc]
	if !ok {
		return nil, fmt.Errorf("no postal-code rule for country %q", cc)
	}

	// Normalize the pattern to RE2
	p = strings.TrimSpace(p)
	if len(p) >= 2 && strings.HasPrefix(p, "/") && strings.HasSuffix(p, "/") {
		p = p[1 : len(p)-1]
	}
	if strings.Contains(p, `\d`) {
		p = strings.ReplaceAll(p, `\d`, `[0-9]`)
	}
	if strings.Contains(p, `\s`) {
		p = strings.ReplaceAll(p, `\s`, `[[:space:]]`)
	}
	if strings.Contains(p, `\w`) {
		p = strings.ReplaceAll(p, `\w`, `[A-Za-z0-9_]`)
	}
	if strings.Contains(p, "(?:") {
		p = strings.ReplaceAll(p, "(?:", "(")
	}

	// Ensure anchors to avoid substring matches
	if !strings.HasPrefix(p, "^") {
		p = "^" + p
	}
	if !strings.HasSuffix(p, "$") {
		p = p + "$"
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, fmt.Errorf("%s: compile error for %q -> %v", cc, p, err)
	}

	postCodeRegexCache.Store(cc, re)
	return re, nil
}

func SanatizeUGC(fl validator.FieldLevel) bool {
	log.Trace("profilesDTOAdapter SanatizeUGC")

	fv := fl.Field()
	if fv.Kind() != reflect.String { // ignore non-strings
		return true
	}
	// Important: pass &dto to validator.Struct(&dto)
	if !fv.CanSet() {
		return true
	}

	s := fv.String()
	sanitized := transport.SanitizeUGC(s)
	fv.SetString(sanitized) // mutate - min, max still checked
	return true
}

func ValidatePostalCode(countryCode, code string) error {
	log.Trace("profilesDTOAdapter ValidatePostalCode")

	cc := strings.ToUpper(strings.TrimSpace(countryCode))
	clean := strings.TrimSpace(code)

	// no postal code required
	if cc == "HK" || cc == "AG" {
		return nil
	}

	if clean == "" {
		return fmt.Errorf("validation error. postal code is required for %s", cc)
	}

	re, err := getPostalRegex(cc)
	if err != nil {
		return err
	}
	if !re.MatchString(clean) {
		return fmt.Errorf("validation error. postal code %q does not match format for %s", clean, cc)
	}
	return nil
}

func rawPatterns() map[string]string {
	log.Trace("profilesDTOAdapter rawPatterns")

	return map[string]string{
		"AF": "/[0-9]{4}/",
		"AL": "/(120|122)[0-9]{2}/",
		"DZ": "/[0-9]{5}/",
		"AS": "/[0-9]{5}/",
		"AD": "^AD[1-7][0-9]{2}$",
		"AO": "/^\\d{5}$/",
		"AI": "/AI-2640/",
		"AG": "/\\d{5}(?:[-\\s]\\d{4})?/",
		"AR": "/[A-Z]{1}[0-9]{4}[A-Z]{3}/",
		"AM": "/[0-9]{4}/",
		"AW": "/^\\d{5}$/",
		"AU": "/[0-9]{4}/",
		"AT": "/[0-9]{4}/",
		"AZ": "/[0-9]{4}/",
		"BS": "/^\\d{5}$/",
		"BH": "/^\\d{5}$/",
		"BD": "/[0-9]{4}/",
		"BB": "/BB[0-9]{5}/",
		"BY": "/[0-9]{6}/",
		"BE": "/[0-9]{4}/",
		"BZ": "/^\\d{5}$/",
		"BJ": "/^\\d{5}$/",
		"BM": "/[A-Z]{2}[0-9]{2}/",
		"BT": "/[0-9]{5}/",
		"BO": "/^\\d{5}$/",
		"BQ": "/^\\d{5}$/",
		"BA": "/[0-9]{5}/",
		"BW": "/^\\d{5}$/",
		"BR": "/[0-9]{5}-[0-9]{3}/",
		"BN": "/[A-Z]{2}[0-9]{4}/",
		"BG": "/[0-9]{4}/",
		"BF": "/^\\d{5}$/",
		"BI": "/^\\d{5}$/",
		"KH": "/[0-9]{5}/",
		"CM": "/^\\d{5}$/",
		"CA": "/[A-Z][0-9][A-Z] ?[0-9][A-Z][0-9]/",
		"CI": "/^\\d{5}$/",
		"CV": "/[0-9]{4}/",
		"KY": "^(KY[1-4])-[0-9]{4}$",
		"CF": "/^\\d{5}$/",
		"TD": "/^\\d{5}$/",
		"CL": "/[0-9]{7}/",
		"CN": "/[0-9]{6}/",
		"CO": "/[0-9]{6}/",
		"KM": "/^\\d{5}$/",
		"CG": "/^\\d{5}$/",
		"CD": "/^\\d{5}$/",
		"CK": "/^\\d{5}$/",
		"CR": "/[0-9]{5}/",
		"HR": "/[0-9]{5}/",
		"CU": "/[0-9]{5}/",
		"CW": "/^\\d{5}$/",
		"CY": "/[0-9]{4}/",
		"CZ": "^[0-9]{3}[[:space:]]?[0-9]{2}$",
		"DK": "^[0-9]{4}$",
		"DJ": "/^\\d{5}$/",
		"DM": "/^\\d{5}$/",
		"DO": "/[0-9]{5}/",
		"TL": "/^\\d{5}$/",
		"EC": "/[0-9]{6}/",
		"EG": "/[0-9]{5}/",
		"SV": "/[0-9]{4}/",
		"ER": "/^\\d{5}$/",
		"EE": "/[0-9]{5}/",
		"ET": "/[0-9]{4}/",
		"FK": "/FIQQ 1ZZ/",
		"FO": "/[0-9]{3}/",
		"FJ": "/^\\d{5}$/",
		"FI": "/[0-9]{5}/",
		"FR": "/[0-9]{5}/",
		"PF": "/[0-9]{5}/",
		"GA": "/^\\d{5}$/",
		"GM": "/^\\d{5}$/",
		"GE": "/^\\d{5}$/",
		"DE": "/[0-9]{5}/",
		"GH": "/^\\d{5}$/",
		"GI": "^GX11[[:space:]]1AA$",
		"GR": "/[0-9]{3} [0-9]{2}/",
		"GL": "/[0-9]{4}/",
		"GD": "/^\\d{5}$/",
		"GP": "/971[0-9]{2}/",
		"GU": "^[0-9]{5}(?:-[0-9]{4})?$",
		"GT": "/[0-9]{5}/",
		"GG": "([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))[[:space:]]?[0-9][A-Za-z]{2})",
		"GW": "/[0-9]{4}/",
		"GQ": "/^\\d{5}$/",
		"GN": "/[0-9]{3}/",
		"GY": "/^\\d{5}$/",
		"GF": "/973[0-9]{2}/",
		"HT": "/[0-9]{4}/",
		"HN": "/[0-9]{5}/",
		"HK": "/^\\d{5}$/",
		"HU": "/[0-9]{4}/",
		"IS": "/[0-9]{3}/",
		"IN": "/[1-9][0-9]{5}/",
		"ID": "/[0-9]{5}/",
		"IR": "/[0-9]{5}/",
		"IQ": "/[0-9]{5}/",
		"IE": "(?:([AC-FHKNPRTV-Ya-c-fhknprt v- y][0-9]{2})|D6W)[ -]?[0-9AC-FHKNPRTV-Ya-c-fhknprtv-y]{4}",
		"IL": "/[0-9]{5}|[0-9]{7}/",
		"IT": "/[0-9]{5}/",
		"JM": "/^\\d{5}$/",
		"JP": "/[0-9]{3}-[0-9]{4}/",
		"JE": "([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))[[:space:]]?[0-9][A-Za-z]{2})",
		"JO": "/[0-9]{5}/",
		"KZ": "/[0-9]{6}/",
		"KE": "/[0-9]{5}/",
		"KI": "/^\\d{5}$/",
		"KR": "/[0-9]{5}/",
		"KP": "/^\\d{5}$/",
		"XK": "/[0-9]{5}/",
		"KW": "/[0-9]{5}/",
		"KG": "/[0-9]{6}/",
		"LA": "/[0-9]{5}/",
		"LV": "/LV-[0-9]{4}/",
		"LB": "/[0-9]{4} [0-9]{4}/",
		"LS": "/[0-9]{3}/",
		"LR": "/[0-9]{4}/",
		"LY": "/^\\d{5}$/",
		"LI": "/[0-9]{4}/",
		"LT": "/LT-[0-9]{5}/",
		"LU": "/[0-9]{4}/",
		"MO": "/^\\d{5}$/",
		"MK": "/[0-9]{4}/",
		"MG": "/[0-9]{3}/",
		"MW": "/^\\d{5}$/",
		"MY": "/[0-9]{5}/",
		"MV": "/[0-9]{5}/",
		"ML": "/^\\d{5}$/",
		"MT": "/[A-Z]{3} [0-9]{4}/",
		"MH": "^[0-9]{5}(?:-[0-9]{4})?$",
		"MQ": "/972[0-9]{2}/",
		"MR": "/^\\d{5}$/",
		"MU": "/[0-9]{5}/",
		"YT": "/976[0-9]{2}/",
		"MX": "/[0-9]{5}/",
		"MD": "/MD-?[0-9]{4}/",
		"MC": "/980[0-9]{2}/",
		"MN": "/[0-9]{5}/",
		"ME": "/[0-9]{5}/",
		"MS": "/MSR [0-9]{4}/",
		"MA": "/[0-9]{5}/",
		"MZ": "/[0-9]{4}/",
		"MM": "/[0-9]{5}/",
		"NA": "/^\\d{5}$/",
		"NR": "/^\\d{5}$/",
		"NP": "/[0-9]{5}/",
		"NL": "^[1-9][0-9]{3}\\s?[A-Z]{2}$",
		"NC": "/988[0-9]{2}/",
		"NZ": "/[0-9]{4}/",
		"NI": "/^\\d{5}$/",
		"NE": "/[0-9]{4}/",
		"NG": "/[0-9]{6}/",
		"NU": "/^\\d{5}$/",
		"MP": "/96950/",
		"NO": "/[0-9]{4}/",
		"OM": "/[0-9]{3}/",
		"PK": "/[0-9]{5}/",
		"PW": "^[0-9]{5}(?:-[0-9]{4})?$",
		"PA": "/[0-9]{4}/",
		"PG": "/[0-9]{3}/",
		"PY": "/[0-9]{4}/",
		"PE": "/[0-9]{5}/",
		"PH": "/[0-9]{4}/",
		"PL": "/[0-9]{2}-[0-9]{3}/",
		"PT": "/[0-9]{4}-[0-9]{3}/",
		"PR": "^[0-9]{5}(?:-[0-9]{4})?$",
		"QA": "/^\\d{5}$/",
		"RE": "/974[0-9]{2}/",
		"RO": "/[0-9]{6}/",
		"RU": "/[0-9]{6}/",
		"RW": "/^\\d{5}$/",
		"WS": "/WS[0-9]{4}/",
		"ST": "/^\\d{5}$/",
		"SA": "/[0-9]{5}(-[0-9]{4})?/",
		"SN": "/[0-9]{5}/",
		"RS": "/[0-9]{5}/",
		"SC": "/^\\d{5}$/",
		"SL": "/^\\d{5}$/",
		"SG": "/[0-9]{6}/",
		"SK": "^[0-9]{3}[[:space:]]?[0-9]{2}$",
		"SI": "/[0-9]{4}/",
		"SB": "/^\\d{5}$/",
		"SO": "/[A-Z]{2} [0-9]{5}/",
		"ZA": "/[0-9]{4}/",
		"SS": "/^\\d{5}$/",
		"ES": "/[0-9]{5}/",
		"LK": "/[0-9]{4}/",
		"BL": "/[0-9]{5}/",
		"VI": "^[0-9]{5}(?:-[0-9]{4})?$",
		"SE": "^(?:[0-9]{3}\\s?[0-9]{2})$",
		"SH": "/STHL 1ZZ/",
		"KN": "/[A-Z]{2}[0-9]{4}/",
		"LC": "/[A-Z]{2}[0-9]{2} [0-9]{3}/",
		"SX": "/^\\d{5}$/",
		"VC": "/VC[0-9]{4}/",
		"SD": "/[0-9]{5}/",
		"SR": "/^\\d{5}$/",
		"SZ": "/[A-Z]{1}[0-9]{3}/",
		"CH": "^[0-9]{4}$",
		"SY": "/^\\d{5}$/",
		"TW": "/[0-9]{3}(-[0-9]{2})?/",
		"TZ": "/[0-9]{5}/",
		"TH": "/[0-9]{5}/",
		"TG": "/^\\d{5}$/",
		"TO": "/^\\d{5}$/",
		"VG": "/VG[0-9]{4}/",
		"TT": "/[0-9]{6}/",
		"TN": "/[0-9]{4}/",
		"TR": "/[0-9]{5}/",
		"TM": "/[0-9]{6}/",
		"TC": "/TKCA 1ZZ/",
		"TV": "/^\\d{5}$/",
		"UG": "/^\\d{5}$/",
		"UA": "/[0-9]{5}/",
		"AE": "/^\\d{5}$/",
		"GB": "([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))[[:space:]]?[0-9][A-Za-z]{2})",
		"US": "^[0-9]{5}(?:-[0-9]{4})?$",
		"UY": "/[0-9]{5}/",
		"UZ": "/[0-9]{6}/",
		"VU": "/^\\d{5}$/",
		"VE": "/[0-9]{4}(-[A-Z]{1})?/",
		"VN": "/[0-9]{6}/",
		"YE": "/^\\d{5}$/",
		"ZM": "/[0-9]{5}/",
		"ZW": "/^\\d{5}$/",
	}
}
