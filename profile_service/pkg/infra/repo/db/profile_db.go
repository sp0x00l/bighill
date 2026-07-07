package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	dbConn "lib/shared_lib/db"
	"lib/shared_lib/uuidutil"
	"profile_service/pkg/domain"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrProfileNotFound       = domain.ErrProfileNotFound
	ErrOAuthIdentityNotFound = domain.ErrOAuthIdentityNotFound
	ErrEmailNotVerified      = domain.ErrEmailNotVerified
)

type ProfileDB interface {
	Save(ctx context.Context, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	SaveTx(ctx context.Context, tx pgx.Tx, profile *domain.ProfileAccount, idempotencyKey uuid.UUID) error
	Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdateTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdateHuggingFaceToken(ctx context.Context, userID uuid.UUID, ciphertext string) (*domain.Profile, error)
	UpdateHuggingFaceTokenTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, ciphertext string) (*domain.Profile, error)
	UpdatePassword(ctx context.Context, userID uuid.UUID, newPassword string) error
	VerifyEmail(ctx context.Context, token string) (*domain.Profile, error)
	VerifyEmailTx(ctx context.Context, tx pgx.Tx, token string) (*domain.Profile, error)
	Read(ctx context.Context, userID uuid.UUID) (*domain.Profile, error)
	ReadTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (*domain.Profile, error)
	ReadByVerifyToken(ctx context.Context, token string) (*domain.Profile, error)
	ReadPasswordHash(ctx context.Context, email string) (uuid.UUID, string, error)
	ReadOAuthProfileIDByProviderSubject(ctx context.Context, provider, subject string) (uuid.UUID, error)
	ReadProfileIDByEmail(ctx context.Context, email string) (uuid.UUID, error)
	CreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	CreateOAuthProfileTx(ctx context.Context, tx pgx.Tx, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error
	SaveOAuthIdentityTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, identity domain.OAuthIdentity) error
	Delete(ctx context.Context, userID uuid.UUID) error
	DeleteTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) error
	ReadDefaultMembership(ctx context.Context, userID uuid.UUID) (*domain.OrganizationMembership, error)
	ReadMembership(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) (*domain.OrganizationMembership, error)
	ReadOrganization(ctx context.Context, orgID uuid.UUID) (*domain.Organization, error)
	ListMemberships(ctx context.Context, orgID uuid.UUID) ([]*domain.OrganizationMembership, error)
	UpsertMembership(ctx context.Context, tx pgx.Tx, membership *domain.OrganizationMembership) (*domain.OrganizationMembership, error)
	DeleteMembership(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, userID uuid.UUID) error
}

type profileDB struct {
	dbConn.Database
}

type profileExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func NewProfileDB(db *dbConn.Database) ProfileDB {
	return &profileDB{
		*db,
	}
}

func (db *profileDB) executor(tx pgx.Tx) profileExecutor {
	log.Trace("ProfileDB executor")

	if tx != nil {
		return tx
	}
	return db.Pool
}

func (db *profileDB) Save(ctx context.Context, profileAccount *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	log.Trace("ProfileDB Save")

	return db.SaveTx(ctx, nil, profileAccount, idempotencyKey)
}

func (db *profileDB) SaveTx(ctx context.Context, tx pgx.Tx, profileAccount *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	log.Trace("ProfileDB SaveTx")

	dao := ToDAOProfileAccount(profileAccount)
	dao["idempotency_key"] = pgtype.UUID{Bytes: idempotencyKey, Valid: true}

	var profileID string
	var sqlStatementProfile = `
	INSERT INTO ` + db.Name + `.profiles (id, idempotency_key, email, phone_number, country_code, password_hash, email_verified, email_verify_token_hash, email_verify_expires_at)
	VALUES (uuid_generate_v4(), @idempotency_key, @email, @phone_number, @country_code, @password_hash, @email_verified, @email_verify_token_hash, @email_verify_expires_at)
	RETURNING id;`

	err := db.executor(tx).QueryRow(ctx,
		sqlStatementProfile,
		dao,
	).Scan(&profileID)
	if err != nil {
		profileAccount.ID = uuid.Nil
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			param := strings.TrimPrefix(pgErr.ConstraintName, "profiles_")
			param = strings.ReplaceAll(strings.TrimSuffix(param, "_key"), "_", " ")
			return fmt.Errorf("%w: %s already exists", domain.ErrProfileAlreadyExists, param)
		}
		db.LogPoolStatsOnError(ctx, "database error. Failed to insert profile", err)
		return fmt.Errorf("database error. Failed to insert profile: %w", err)
	}
	parsedProfileID, err := uuidutil.Parse("profile id", profileID)
	if err != nil {
		profileAccount.ID = uuid.Nil
		return fmt.Errorf("database error. insert profile returned invalid id: %w", err)
	}
	profileAccount.ID = parsedProfileID
	orgID, err := db.createDefaultOrganizationTx(ctx, tx, parsedProfileID, profileAccount.Email)
	if err != nil {
		profileAccount.ID = uuid.Nil
		return err
	}
	profileAccount.DefaultOrgID = orgID
	return nil
}

func (db *profileDB) Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error) {
	log.Trace("ProfileDB Update")

	return db.UpdateTx(ctx, nil, userID, profile)
}

func (db *profileDB) createDefaultOrganizationTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, email string) (uuid.UUID, error) {
	log.Trace("ProfileDB createDefaultOrganizationTx")

	orgID := uuid.New()
	displayName := defaultOrganizationName(email)
	exec := db.executor(tx)
	if _, err := exec.Exec(ctx, `
		INSERT INTO `+db.Name+`.organizations (id, display_name, created_by_user_id)
		VALUES (@org_id, @display_name, @created_by_user_id);`,
		pgx.NamedArgs{
			"org_id":             pgtype.UUID{Bytes: orgID, Valid: true},
			"display_name":       pgtype.Text{String: displayName, Valid: true},
			"created_by_user_id": pgtype.UUID{Bytes: userID, Valid: true},
		},
	); err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to create default organization: %w", err)
	}
	if _, err := exec.Exec(ctx, `
		INSERT INTO `+db.Name+`.organization_memberships (org_id, user_id, role, status, created_by_user_id)
		VALUES (@org_id, @user_id, 'org_admin', 'active', @created_by_user_id);`,
		pgx.NamedArgs{
			"org_id":             pgtype.UUID{Bytes: orgID, Valid: true},
			"user_id":            pgtype.UUID{Bytes: userID, Valid: true},
			"created_by_user_id": pgtype.UUID{Bytes: userID, Valid: true},
		},
	); err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to create default organization membership: %w", err)
	}
	if _, err := exec.Exec(ctx, `
		UPDATE `+db.Name+`.profiles SET default_org_id = @org_id WHERE id = @user_id;`,
		pgx.NamedArgs{
			"org_id":  pgtype.UUID{Bytes: orgID, Valid: true},
			"user_id": pgtype.UUID{Bytes: userID, Valid: true},
		},
	); err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to set default organization: %w", err)
	}
	return orgID, nil
}

func defaultOrganizationName(email string) string {
	log.Trace("defaultOrganizationName")

	email = strings.TrimSpace(email)
	if email == "" {
		return "Default Organization"
	}
	return email + " Organization"
}

func (db *profileDB) UpdateTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error) {
	log.Trace("ProfileDB UpdateTx")

	// important to use userID here, not profile.ID to avoid any potential security issues
	dao := ToDAO(profile, userID)

	// no password update here, password is updated separately
	var sqlStatementProfile = `
	UPDATE ` + db.Name + `.profiles
	SET email = @email, first_name = @first_name, last_name = @last_name, phone_number = @phone_number,
	date_of_birth = @date_of_birth, country_code = @country_code, address_line_1 = @address_line_1,
	address_line_2 = @address_line_2, city = @city, state = @state, postal_code = @postal_code, country = @country
	WHERE id = @id AND deleted = false RETURNING id, default_org_id, email, first_name, last_name, phone_number, date_of_birth, country_code,
	address_line_1, address_line_2, city, state, postal_code, country, huggingface_token_ciphertext, email_verified;`

	var updatedProfile ProfileDAO = ProfileDAO{}
	row := db.executor(tx).QueryRow(ctx, sqlStatementProfile, dao)
	switch err := row.Scan(&updatedProfile.ID, &updatedProfile.DefaultOrgID, &updatedProfile.Email, &updatedProfile.FirstName, &updatedProfile.LastName,
		&updatedProfile.PhoneNumber, &updatedProfile.DateOfBirth, &updatedProfile.CountryCode,
		&updatedProfile.AddressLine1, &updatedProfile.AddressLine2, &updatedProfile.City, &updatedProfile.State,
		&updatedProfile.PostalCode, &updatedProfile.Country, &updatedProfile.HuggingFaceTokenCiphertext, &updatedProfile.EmailVerified); err {
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	case nil:
		profileModel, err := FromDAO(&updatedProfile)
		if err != nil {
			return nil, err
		}
		return profileModel, nil
	default:
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			param := strings.TrimPrefix(pgErr.ConstraintName, "profiles_")
			param = strings.ReplaceAll(strings.TrimSuffix(param, "_key"), "_", " ")
			return nil, fmt.Errorf("%w: %s already exists", domain.ErrProfileAlreadyExists, param)
		}
		db.LogPoolStatsOnError(ctx, "database error. Failed to update profile", err)
		return nil, fmt.Errorf("database error. Failed to update profile: %w", err)
	}

}

func (db *profileDB) UpdateHuggingFaceToken(ctx context.Context, userID uuid.UUID, ciphertext string) (*domain.Profile, error) {
	log.Trace("ProfileDB UpdateHuggingFaceToken")

	return db.UpdateHuggingFaceTokenTx(ctx, nil, userID, ciphertext)
}

func (db *profileDB) UpdateHuggingFaceTokenTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, ciphertext string) (*domain.Profile, error) {
	log.Trace("ProfileDB UpdateHuggingFaceTokenTx")

	var profileDAO ProfileDAO
	err := db.executor(tx).QueryRow(ctx, `
		UPDATE `+db.Name+`.profiles
		SET huggingface_token_ciphertext = @huggingface_token_ciphertext
		WHERE id = @id AND deleted = false
		RETURNING id, default_org_id, email, first_name, last_name, phone_number, date_of_birth, country_code,
			address_line_1, address_line_2, city, state, postal_code, country, huggingface_token_ciphertext, email_verified;`,
		pgx.NamedArgs{
			"id":                           pgtype.UUID{Bytes: userID, Valid: true},
			"huggingface_token_ciphertext": pgtype.Text{String: ciphertext, Valid: true},
		},
	).Scan(
		&profileDAO.ID,
		&profileDAO.DefaultOrgID,
		&profileDAO.Email,
		&profileDAO.FirstName,
		&profileDAO.LastName,
		&profileDAO.PhoneNumber,
		&profileDAO.DateOfBirth,
		&profileDAO.CountryCode,
		&profileDAO.AddressLine1,
		&profileDAO.AddressLine2,
		&profileDAO.City,
		&profileDAO.State,
		&profileDAO.PostalCode,
		&profileDAO.Country,
		&profileDAO.HuggingFaceTokenCiphertext,
		&profileDAO.EmailVerified,
	)
	switch err {
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	case nil:
		return FromDAO(&profileDAO)
	default:
		db.LogPoolStatsOnError(ctx, "database error. Failed to update hugging face token", err)
		return nil, fmt.Errorf("database error. Failed to update hugging face token: %w", err)
	}
}

func (db *profileDB) UpdatePassword(ctx context.Context, userID uuid.UUID, newPassword string) error {
	log.Trace("ProfileDB UpdatePassword")

	var sqlStatement = `
	UPDATE ` + db.Name + `.profiles
	SET password_hash = @password_hash
	WHERE id = @id AND deleted = false;`

	cmdTag, err := db.Pool.Exec(ctx, sqlStatement, pgx.NamedArgs{
		"id":            pgtype.UUID{Bytes: userID, Valid: true},
		"password_hash": pgtype.Text{String: newPassword, Valid: true},
	})
	if err != nil {
		db.LogPoolStatsOnError(ctx, "database error. Failed to update password", err)
		return fmt.Errorf("database error. Failed to update password: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("password update failed: %w", ErrProfileNotFound)
	}
	return nil
}

func (db *profileDB) VerifyEmail(ctx context.Context, token string) (*domain.Profile, error) {
	log.Trace("ProfileDB VerifyEmail")

	return db.VerifyEmailTx(ctx, nil, token)
}

func (db *profileDB) VerifyEmailTx(ctx context.Context, tx pgx.Tx, token string) (*domain.Profile, error) {
	log.Trace("ProfileDB VerifyEmailTx")

	var profileDAO ProfileDAO
	err := db.executor(tx).QueryRow(ctx, `
		UPDATE `+db.Name+`.profiles
		SET email_verified = true, email_verify_token_hash = NULL, email_verify_expires_at = NULL
		WHERE id = (
			SELECT id
			FROM `+db.Name+`.profiles
			WHERE email_verify_token_hash = @email_verify_token_hash
				AND email_verified = false
				AND email_verify_expires_at > CURRENT_TIMESTAMP
				AND deleted = false
			ORDER BY email_verify_expires_at DESC, id DESC
			LIMIT 1
		)
		RETURNING id, default_org_id, email, first_name, last_name, phone_number,
			date_of_birth, country_code, address_line_1, address_line_2, city, state,
			postal_code, country, huggingface_token_ciphertext, email_verified;`,
		pgx.NamedArgs{
			"email_verify_token_hash": pgtype.Text{String: hashVerificationToken(token), Valid: true},
		},
	).Scan(
		&profileDAO.ID,
		&profileDAO.DefaultOrgID,
		&profileDAO.Email,
		&profileDAO.FirstName,
		&profileDAO.LastName,
		&profileDAO.PhoneNumber,
		&profileDAO.DateOfBirth,
		&profileDAO.CountryCode,
		&profileDAO.AddressLine1,
		&profileDAO.AddressLine2,
		&profileDAO.City,
		&profileDAO.State,
		&profileDAO.PostalCode,
		&profileDAO.Country,
		&profileDAO.HuggingFaceTokenCiphertext,
		&profileDAO.EmailVerified,
	)
	switch err {
	case pgx.ErrNoRows:
		return nil, fmt.Errorf("%w for token", ErrProfileNotFound)
	case nil:
		return FromDAO(&profileDAO)
	default:
		return nil, fmt.Errorf("database error. Failed to verify email: %w", err)
	}
}

func (db *profileDB) ReadByVerifyToken(ctx context.Context, token string) (*domain.Profile, error) {
	log.Trace("ProfileDB ReadByVerifyToken")

	var profileDAO ProfileDAO
	err := db.Pool.QueryRow(ctx, `
	SELECT id, default_org_id, email, first_name, last_name, phone_number,
	date_of_birth, country_code, address_line_1, address_line_2, city, state,
	postal_code, country, huggingface_token_ciphertext, email_verified
	FROM `+db.Name+`.profiles
	WHERE email_verify_token_hash = @email_verify_token_hash AND deleted = false;`,
		pgx.NamedArgs{
			"email_verify_token_hash": pgtype.Text{String: hashVerificationToken(token), Valid: true},
		},
	).Scan(
		&profileDAO.ID,
		&profileDAO.DefaultOrgID,
		&profileDAO.Email,
		&profileDAO.FirstName,
		&profileDAO.LastName,
		&profileDAO.PhoneNumber,
		&profileDAO.DateOfBirth,
		&profileDAO.CountryCode,
		&profileDAO.AddressLine1,
		&profileDAO.AddressLine2,
		&profileDAO.City,
		&profileDAO.State,
		&profileDAO.PostalCode,
		&profileDAO.Country,
		&profileDAO.HuggingFaceTokenCiphertext,
		&profileDAO.EmailVerified,
	)
	switch err {
	case pgx.ErrNoRows:
		return nil, fmt.Errorf("%w for token", ErrProfileNotFound)
	case nil:
		return FromDAO(&profileDAO)
	default:
		db.LogPoolStatsOnError(ctx, "database error. Failed to retrieve profile by verify token", err)
		return nil, fmt.Errorf("database error. Failed to retrieve profile by verify token: %w", err)
	}
}

func (db *profileDB) Read(ctx context.Context, userID uuid.UUID) (*domain.Profile, error) {
	log.Trace("ProfileDB Read")

	return db.ReadTx(ctx, nil, userID)
}

func (db *profileDB) ReadTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (*domain.Profile, error) {
	log.Trace("ProfileDB ReadTx")

	var profileDao ProfileDAO
	var sqlStatementProfile = `
	SELECT id, default_org_id, email, first_name, last_name, phone_number,
	date_of_birth, country_code, address_line_1, address_line_2, city, state,
	postal_code, country, huggingface_token_ciphertext, email_verified
	FROM ` + db.Name + `.profiles WHERE id = @id AND deleted = false;`

	switch err := db.executor(tx).QueryRow(ctx,
		sqlStatementProfile,
		pgx.NamedArgs{
			"id": pgtype.UUID{Bytes: userID, Valid: true},
		}).Scan(
		&profileDao.ID,
		&profileDao.DefaultOrgID,
		&profileDao.Email,
		&profileDao.FirstName,
		&profileDao.LastName,
		&profileDao.PhoneNumber,
		&profileDao.DateOfBirth,
		&profileDao.CountryCode,
		&profileDao.AddressLine1,
		&profileDao.AddressLine2,
		&profileDao.City,
		&profileDao.State,
		&profileDao.PostalCode,
		&profileDao.Country,
		&profileDao.HuggingFaceTokenCiphertext,
		&profileDao.EmailVerified,
	); err {
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	case nil:
		profileModel, err := FromDAO(&profileDao)
		if err != nil {
			return nil, err
		}
		return profileModel, nil
	default:
		db.LogPoolStatsOnError(ctx, "database error. Failed to retrieve profile", err)
		return nil, fmt.Errorf("database error. Failed to retrieve profile: %w", err)
	}
}

func (db *profileDB) ReadPasswordHash(ctx context.Context, email string) (uuid.UUID, string, error) {
	log.Trace("ProfileDB ReadPasswordHash")

	var userID uuid.UUID
	var passwordHash string
	var emailVerified bool
	var sqlStatement = `
	SELECT id, password_hash, email_verified
	FROM ` + db.Name + `.profiles WHERE email = @email AND deleted = false;`

	switch err := db.Pool.QueryRow(ctx,
		sqlStatement,
		pgx.NamedArgs{
			"email": pgtype.Text{String: email, Valid: true},
		}).Scan(&userID, &passwordHash, &emailVerified); err {
	case pgx.ErrNoRows:
		return uuid.Nil, "", ErrProfileNotFound
	case nil:
		if !emailVerified {
			return uuid.Nil, "", ErrEmailNotVerified
		}
		return userID, passwordHash, nil
	default:
		db.LogPoolStatsOnError(ctx, "database error. Failed to retrieve password hash", err)
		return uuid.Nil, "", fmt.Errorf("database error. Failed to retrieve password hash: %w", err)
	}
}

func (db *profileDB) ReadOAuthProfileIDByProviderSubject(ctx context.Context, provider, subject string) (uuid.UUID, error) {
	log.Trace("ProfileDB ReadOAuthProfileIDByProviderSubject")

	var oauthProfileIDDAO OAuthProfileIDDAO
	err := db.Pool.QueryRow(ctx, `
		SELECT p.id
		FROM `+db.Name+`.oauth_identities oi
		INNER JOIN `+db.Name+`.profiles p ON p.id = oi.profile_id
		WHERE oi.provider = @provider AND oi.provider_subject = @provider_subject AND p.deleted = false;`,
		pgx.NamedArgs{
			"provider":         pgtype.Text{String: strings.ToLower(strings.TrimSpace(provider)), Valid: true},
			"provider_subject": pgtype.Text{String: strings.TrimSpace(subject), Valid: true},
		},
	).Scan(&oauthProfileIDDAO.ProfileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrOAuthIdentityNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to load oauth identity: %w", err)
	}
	return FromDAOOAuthProfileID(&oauthProfileIDDAO), nil
}

func (db *profileDB) ReadProfileIDByEmail(ctx context.Context, email string) (uuid.UUID, error) {
	log.Trace("ProfileDB ReadProfileIDByEmail")

	var profileIDDAO ProfileIDDAO
	err := db.Pool.QueryRow(ctx, `
		SELECT id
		FROM `+db.Name+`.profiles
		WHERE email = @email AND deleted = false;`,
		pgx.NamedArgs{
			"email": pgtype.Text{String: strings.ToLower(strings.TrimSpace(email)), Valid: true},
		},
	).Scan(&profileIDDAO.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrProfileNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to load profile for oauth identity: %w", err)
	}
	return FromDAOProfileID(&profileIDDAO), nil
}

func (db *profileDB) CreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error) {
	log.Trace("ProfileDB CreateOAuthProfile")

	return db.CreateOAuthProfileTx(ctx, nil, identity, passwordHash)
}

func (db *profileDB) CreateOAuthProfileTx(ctx context.Context, tx pgx.Tx, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error) {
	log.Trace("ProfileDB CreateOAuthProfileTx")

	email := strings.ToLower(strings.TrimSpace(identity.Email))
	if email == "" {
		return uuid.Nil, fmt.Errorf("database error. invalid oauth identity")
	}
	identity.Email = email
	identity.FirstName = strings.TrimSpace(identity.FirstName)
	identity.LastName = strings.TrimSpace(identity.LastName)
	dao := ToDAOOAuthProfile(identity, passwordHash)

	var profileID string
	err := db.executor(tx).QueryRow(ctx, `
		INSERT INTO `+db.Name+`.profiles (id, idempotency_key, email, phone_number, country_code, password_hash, first_name, last_name)
		VALUES (uuid_generate_v4(), @idempotency_key, @email, @phone_number, @country_code, @password_hash, @first_name, @last_name)
		RETURNING id;`,
		dao,
	).Scan(&profileID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("database error. failed to create oauth profile: %w", err)
	}
	parsedProfileID, err := uuidutil.Parse("profile id", profileID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("database error. create oauth profile returned invalid id: %w", err)
	}
	if _, err := db.createDefaultOrganizationTx(ctx, tx, parsedProfileID, identity.Email); err != nil {
		return uuid.Nil, err
	}
	return parsedProfileID, nil
}

func (db *profileDB) SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error {
	log.Trace("ProfileDB SaveOAuthIdentity")

	return db.SaveOAuthIdentityTx(ctx, nil, userID, identity)
}

func (db *profileDB) SaveOAuthIdentityTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID, identity domain.OAuthIdentity) error {
	log.Trace("ProfileDB SaveOAuthIdentityTx")

	dao := ToDAOOAuthIdentity(userID, domain.OAuthIdentity{
		Provider:      strings.ToLower(strings.TrimSpace(identity.Provider)),
		Subject:       strings.TrimSpace(identity.Subject),
		Email:         strings.ToLower(strings.TrimSpace(identity.Email)),
		EmailVerified: identity.EmailVerified,
	})

	_, err := db.executor(tx).Exec(ctx, `
		INSERT INTO `+db.Name+`.oauth_identities (id, profile_id, provider, provider_subject, email, email_verified)
		VALUES (uuid_generate_v4(), @profile_id, @provider, @provider_subject, @email, @email_verified)
		ON CONFLICT (provider, provider_subject)
		DO UPDATE SET
			profile_id = EXCLUDED.profile_id,
			email = EXCLUDED.email,
			email_verified = EXCLUDED.email_verified,
			updated_at = CURRENT_TIMESTAMP;`,
		dao,
	)
	if err != nil {
		return fmt.Errorf("database error. failed to store oauth identity: %w", err)
	}
	return nil
}

func (db *profileDB) Delete(ctx context.Context, userID uuid.UUID) error {
	log.Trace("ProfileDB Delete")

	return db.DeleteTx(ctx, nil, userID)
}

func (db *profileDB) DeleteTx(ctx context.Context, tx pgx.Tx, userID uuid.UUID) error {
	log.Trace("ProfileDB DeleteTx")

	var sqlStatement = `
	UPDATE ` + db.Name + `.profiles
	SET deleted = true
	WHERE id = @id AND deleted = false;`

	cmdTag, err := db.executor(tx).Exec(ctx, sqlStatement, pgx.NamedArgs{
		"id": pgtype.UUID{Bytes: userID, Valid: true},
	})
	if err != nil {
		db.LogPoolStatsOnError(ctx, "database error. Failed to delete profile", err)
		return fmt.Errorf("database error. Failed to delete profile: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrProfileNotFound
	}
	return nil
}

func (db *profileDB) ReadDefaultMembership(ctx context.Context, userID uuid.UUID) (*domain.OrganizationMembership, error) {
	log.Trace("ProfileDB ReadDefaultMembership")

	var membership OrganizationMembershipDAO
	err := db.Pool.QueryRow(ctx, `
		SELECT m.org_id, m.user_id, p.email, m.role::text, m.status::text, m.created_by_user_id, m.created_at, m.updated_at
		FROM `+db.Name+`.profiles p
		INNER JOIN `+db.Name+`.organization_memberships m
			ON m.org_id = p.default_org_id AND m.user_id = p.id
		WHERE p.id = @user_id AND p.deleted = false AND m.status = 'active';`,
		pgx.NamedArgs{"user_id": pgtype.UUID{Bytes: userID, Valid: true}},
	).Scan(
		&membership.OrgID,
		&membership.UserID,
		&membership.Email,
		&membership.Role,
		&membership.Status,
		&membership.CreatedByUserID,
		&membership.CreatedAt,
		&membership.UpdatedAt,
	)
	switch err {
	case nil:
		return FromDAOOrganizationMembership(&membership), nil
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	default:
		return nil, fmt.Errorf("database error. failed to read default membership: %w", err)
	}
}

func (db *profileDB) ReadMembership(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) (*domain.OrganizationMembership, error) {
	log.Trace("ProfileDB ReadMembership")

	var membership OrganizationMembershipDAO
	err := db.Pool.QueryRow(ctx, `
		SELECT m.org_id, m.user_id, p.email, m.role::text, m.status::text, m.created_by_user_id, m.created_at, m.updated_at
		FROM `+db.Name+`.organization_memberships m
		INNER JOIN `+db.Name+`.profiles p ON p.id = m.user_id AND p.deleted = false
		WHERE m.org_id = @org_id AND m.user_id = @user_id;`,
		pgx.NamedArgs{
			"org_id":  pgtype.UUID{Bytes: orgID, Valid: true},
			"user_id": pgtype.UUID{Bytes: userID, Valid: true},
		},
	).Scan(
		&membership.OrgID,
		&membership.UserID,
		&membership.Email,
		&membership.Role,
		&membership.Status,
		&membership.CreatedByUserID,
		&membership.CreatedAt,
		&membership.UpdatedAt,
	)
	switch err {
	case nil:
		return FromDAOOrganizationMembership(&membership), nil
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	default:
		return nil, fmt.Errorf("database error. failed to read membership: %w", err)
	}
}

func (db *profileDB) ReadOrganization(ctx context.Context, orgID uuid.UUID) (*domain.Organization, error) {
	log.Trace("ProfileDB ReadOrganization")

	var organization OrganizationDAO
	err := db.Pool.QueryRow(ctx, `
		SELECT id, display_name, created_by_user_id, created_at, updated_at
		FROM `+db.Name+`.organizations
		WHERE id = @org_id AND deleted = false;`,
		pgx.NamedArgs{"org_id": pgtype.UUID{Bytes: orgID, Valid: true}},
	).Scan(
		&organization.ID,
		&organization.DisplayName,
		&organization.CreatedByUserID,
		&organization.CreatedAt,
		&organization.UpdatedAt,
	)
	switch err {
	case nil:
		return FromDAOOrganization(&organization), nil
	case pgx.ErrNoRows:
		return nil, ErrProfileNotFound
	default:
		return nil, fmt.Errorf("database error. failed to read organization: %w", err)
	}
}

func (db *profileDB) ListMemberships(ctx context.Context, orgID uuid.UUID) ([]*domain.OrganizationMembership, error) {
	log.Trace("ProfileDB ListMemberships")

	rows, err := db.Pool.Query(ctx, `
		SELECT m.org_id, m.user_id, p.email, m.role::text, m.status::text, m.created_by_user_id, m.created_at, m.updated_at
		FROM `+db.Name+`.organization_memberships m
		INNER JOIN `+db.Name+`.profiles p ON p.id = m.user_id AND p.deleted = false
		WHERE m.org_id = @org_id
		ORDER BY p.email ASC;`,
		pgx.NamedArgs{"org_id": pgtype.UUID{Bytes: orgID, Valid: true}},
	)
	if err != nil {
		return nil, fmt.Errorf("database error. failed to list memberships: %w", err)
	}
	defer rows.Close()

	memberships := []*domain.OrganizationMembership{}
	for rows.Next() {
		var membership OrganizationMembershipDAO
		if err := rows.Scan(
			&membership.OrgID,
			&membership.UserID,
			&membership.Email,
			&membership.Role,
			&membership.Status,
			&membership.CreatedByUserID,
			&membership.CreatedAt,
			&membership.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("database error. failed to scan membership: %w", err)
		}
		memberships = append(memberships, FromDAOOrganizationMembership(&membership))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("database error. failed to read memberships: %w", err)
	}
	return memberships, nil
}

func (db *profileDB) UpsertMembership(ctx context.Context, tx pgx.Tx, membership *domain.OrganizationMembership) (*domain.OrganizationMembership, error) {
	log.Trace("ProfileDB UpsertMembership")

	if membership == nil {
		return nil, fmt.Errorf("membership is required")
	}
	var dao OrganizationMembershipDAO
	err := db.executor(tx).QueryRow(ctx, `
		WITH upserted AS (
			INSERT INTO `+db.Name+`.organization_memberships (org_id, user_id, role, status, created_by_user_id)
			VALUES (@org_id, @user_id, @role::org_member_role_enum, @status::org_member_status_enum, @created_by_user_id)
			ON CONFLICT (org_id, user_id)
			DO UPDATE SET
				role = EXCLUDED.role,
				status = EXCLUDED.status,
				updated_at = CURRENT_TIMESTAMP
			RETURNING org_id, user_id, role, status, created_by_user_id, created_at, updated_at
		),
		defaulted AS (
			UPDATE `+db.Name+`.profiles p
			SET default_org_id = u.org_id
			FROM upserted u
			WHERE p.id = u.user_id
			  AND p.deleted = false
			  AND u.status = 'active'
			RETURNING p.id
		)
		SELECT org_id, user_id, ''::text AS email, role::text, status::text, created_by_user_id, created_at, updated_at
		FROM upserted;`,
		pgx.NamedArgs{
			"org_id":             pgtype.UUID{Bytes: membership.OrgID, Valid: true},
			"user_id":            pgtype.UUID{Bytes: membership.UserID, Valid: true},
			"role":               pgtype.Text{String: membership.Role, Valid: true},
			"status":             pgtype.Text{String: membership.Status, Valid: true},
			"created_by_user_id": pgtype.UUID{Bytes: membership.CreatedByUserID, Valid: membership.CreatedByUserID != uuid.Nil},
		},
	).Scan(
		&dao.OrgID,
		&dao.UserID,
		&dao.Email,
		&dao.Role,
		&dao.Status,
		&dao.CreatedByUserID,
		&dao.CreatedAt,
		&dao.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("database error. failed to upsert membership: %w", err)
	}
	return FromDAOOrganizationMembership(&dao), nil
}

func (db *profileDB) DeleteMembership(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, userID uuid.UUID) error {
	log.Trace("ProfileDB DeleteMembership")

	tag, err := db.executor(tx).Exec(ctx, `
		UPDATE `+db.Name+`.organization_memberships
		SET status = 'disabled', updated_at = CURRENT_TIMESTAMP
		WHERE org_id = @org_id AND user_id = @user_id;`,
		pgx.NamedArgs{
			"org_id":  pgtype.UUID{Bytes: orgID, Valid: true},
			"user_id": pgtype.UUID{Bytes: userID, Valid: true},
		},
	)
	if err != nil {
		return fmt.Errorf("database error. failed to disable membership: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrProfileNotFound
	}
	return nil
}

func hashVerificationToken(token string) string {
	tokenHash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(tokenHash[:])
}
