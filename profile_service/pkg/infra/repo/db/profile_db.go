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
	Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error)
	UpdateHuggingFaceToken(ctx context.Context, userID uuid.UUID, ciphertext string) (*domain.Profile, error)
	UpdatePassword(ctx context.Context, userID uuid.UUID, newPassword string) error
	VerifyEmail(ctx context.Context, token string) (*domain.Profile, error)
	Read(ctx context.Context, userID uuid.UUID) (*domain.Profile, error)
	ReadByVerifyToken(ctx context.Context, token string) (*domain.Profile, error)
	ReadPasswordHash(ctx context.Context, email string) (uuid.UUID, string, error)
	ReadOAuthProfileIDByProviderSubject(ctx context.Context, provider, subject string) (uuid.UUID, error)
	ReadProfileIDByEmail(ctx context.Context, email string) (uuid.UUID, error)
	CreateOAuthProfile(ctx context.Context, identity domain.OAuthIdentity, passwordHash string) (uuid.UUID, error)
	SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error
	Delete(ctx context.Context, userID uuid.UUID) error
}

type profileDB struct {
	dbConn.Database
}

func NewProfileDB(db *dbConn.Database) ProfileDB {
	return &profileDB{
		*db,
	}
}

func (db *profileDB) Save(ctx context.Context, profileAccount *domain.ProfileAccount, idempotencyKey uuid.UUID) error {
	log.Trace("ProfileDB Save")

	dao := ToDAOProfileAccount(profileAccount)
	dao["idempotency_key"] = pgtype.UUID{Bytes: idempotencyKey, Valid: true}

	var profileID string
	var sqlStatementProfile = `
	INSERT INTO ` + db.Name + `.profiles (id, idempotency_key, email, phone_number, country_code, password_hash, email_verified, email_verify_token_hash, email_verify_expires_at)
	VALUES (uuid_generate_v4(), @idempotency_key, @email, @phone_number, @country_code, @password_hash, @email_verified, @email_verify_token_hash, @email_verify_expires_at)
	RETURNING id;`

	err := db.Pool.QueryRow(ctx,
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
	return nil
}

func (db *profileDB) Update(ctx context.Context, userID uuid.UUID, profile *domain.Profile) (*domain.Profile, error) {
	log.Trace("ProfileDB Update")

	// important to use userID here, not profile.ID to avoid any potential security issues
	dao := ToDAO(profile, userID)

	// no password update here, password is updated separately
	var sqlStatementProfile = `
	UPDATE ` + db.Name + `.profiles
	SET email = @email, first_name = @first_name, last_name = @last_name, phone_number = @phone_number,
	date_of_birth = @date_of_birth, country_code = @country_code, address_line_1 = @address_line_1,
	address_line_2 = @address_line_2, city = @city, state = @state, postal_code = @postal_code, country = @country
	WHERE id = @id AND deleted = false RETURNING id, email, first_name, last_name, phone_number, date_of_birth, country_code,
	address_line_1, address_line_2, city, state, postal_code, country, huggingface_token_ciphertext, email_verified;`

	var updatedProfile ProfileDAO = ProfileDAO{}
	row := db.Pool.QueryRow(ctx, sqlStatementProfile, dao)
	switch err := row.Scan(&updatedProfile.ID, &updatedProfile.Email, &updatedProfile.FirstName, &updatedProfile.LastName,
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

	var profileDAO ProfileDAO
	err := db.Pool.QueryRow(ctx, `
		UPDATE `+db.Name+`.profiles
		SET huggingface_token_ciphertext = @huggingface_token_ciphertext
		WHERE id = @id AND deleted = false
		RETURNING id, email, first_name, last_name, phone_number, date_of_birth, country_code,
			address_line_1, address_line_2, city, state, postal_code, country, huggingface_token_ciphertext, email_verified;`,
		pgx.NamedArgs{
			"id":                           pgtype.UUID{Bytes: userID, Valid: true},
			"huggingface_token_ciphertext": pgtype.Text{String: ciphertext, Valid: true},
		},
	).Scan(
		&profileDAO.ID,
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

	var profileDAO ProfileDAO
	err := db.Pool.QueryRow(ctx, `
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
		RETURNING id, email, first_name, last_name, phone_number,
			date_of_birth, country_code, address_line_1, address_line_2, city, state,
			postal_code, country, huggingface_token_ciphertext, email_verified;`,
		pgx.NamedArgs{
			"email_verify_token_hash": pgtype.Text{String: hashVerificationToken(token), Valid: true},
		},
	).Scan(
		&profileDAO.ID,
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
	SELECT id, email, first_name, last_name, phone_number,
	date_of_birth, country_code, address_line_1, address_line_2, city, state,
	postal_code, country, huggingface_token_ciphertext, email_verified
	FROM `+db.Name+`.profiles
	WHERE email_verify_token_hash = @email_verify_token_hash AND deleted = false;`,
		pgx.NamedArgs{
			"email_verify_token_hash": pgtype.Text{String: hashVerificationToken(token), Valid: true},
		},
	).Scan(
		&profileDAO.ID,
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

	var profileDao ProfileDAO
	var sqlStatementProfile = `
	SELECT id, email, first_name, last_name, phone_number,
	date_of_birth, country_code, address_line_1, address_line_2, city, state,
	postal_code, country, huggingface_token_ciphertext, email_verified
	FROM ` + db.Name + `.profiles WHERE id = @id AND deleted = false;`

	switch err := db.Pool.QueryRow(ctx,
		sqlStatementProfile,
		pgx.NamedArgs{
			"id": pgtype.UUID{Bytes: userID, Valid: true},
		}).Scan(
		&profileDao.ID,
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

	email := strings.ToLower(strings.TrimSpace(identity.Email))
	if email == "" {
		return uuid.Nil, fmt.Errorf("database error. invalid oauth identity")
	}
	identity.Email = email
	identity.FirstName = strings.TrimSpace(identity.FirstName)
	identity.LastName = strings.TrimSpace(identity.LastName)
	dao := ToDAOOAuthProfile(identity, passwordHash)

	var profileID string
	err := db.Pool.QueryRow(ctx, `
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
	return parsedProfileID, nil
}

func (db *profileDB) SaveOAuthIdentity(ctx context.Context, userID uuid.UUID, identity domain.OAuthIdentity) error {
	log.Trace("ProfileDB SaveOAuthIdentity")

	dao := ToDAOOAuthIdentity(userID, domain.OAuthIdentity{
		Provider:      strings.ToLower(strings.TrimSpace(identity.Provider)),
		Subject:       strings.TrimSpace(identity.Subject),
		Email:         strings.ToLower(strings.TrimSpace(identity.Email)),
		EmailVerified: identity.EmailVerified,
	})

	_, err := db.Pool.Exec(ctx, `
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

	var sqlStatement = `
	UPDATE ` + db.Name + `.profiles
	SET deleted = true
	WHERE id = @id AND deleted = false;`

	cmdTag, err := db.Pool.Exec(ctx, sqlStatement, pgx.NamedArgs{
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

func hashVerificationToken(token string) string {
	tokenHash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(tokenHash[:])
}
