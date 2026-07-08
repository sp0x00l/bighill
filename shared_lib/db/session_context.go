package database

import (
	"context"
	"fmt"
	"time"

	"lib/shared_lib/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

const resetSessionContextTimeout = 5 * time.Second

type sessionContextExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func configureTenantSessionHooks(config *pgxpool.Config, dbName string) {
	beforeAcquire := config.BeforeAcquire
	afterRelease := config.AfterRelease

	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		if beforeAcquire != nil && !beforeAcquire(ctx, conn) {
			return false
		}
		if err := applyConnectionSessionContext(ctx, conn, dbName); err != nil {
			log.WithContext(ctx).WithError(err).Error("failed to apply database session context")
			return false
		}
		return true
	}

	config.AfterRelease = func(conn *pgx.Conn) bool {
		resetCtx, cancel := context.WithTimeout(context.Background(), resetSessionContextTimeout)
		defer cancel()
		if err := resetConnectionSessionContext(resetCtx, conn); err != nil {
			log.WithError(err).Error("failed to reset database session context")
			return false
		}
		if afterRelease != nil {
			return afterRelease(conn)
		}
		return true
	}
}

func applyConnectionSessionContext(ctx context.Context, conn sessionContextExecutor, dbName string) error {
	if ctxutil.IsSystemContext(ctx) {
		return setConnectionSessionContext(ctx, conn, "", "", "true")
	}
	orgIDText := ""
	if orgID, ok := ctxutil.OrgID(ctx); ok {
		orgIDText = orgID.String()
	}
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		tenantIDText := tenantID.String()
		return setConnectionSessionContext(ctx, conn, tenantIDText, orgIDText, "")
	}
	if orgIDText != "" {
		return setConnectionSessionContext(ctx, conn, "", orgIDText, "")
	}
	return resetConnectionSessionContext(ctx, conn)
}

func resetConnectionSessionContext(ctx context.Context, conn sessionContextExecutor) error {
	return setConnectionSessionContext(ctx, conn, "", "", "")
}

func setConnectionSessionContext(ctx context.Context, conn sessionContextExecutor, userID string, orgID string, systemContext string) error {
	if _, err := conn.Exec(ctx,
		`SELECT set_config('app.current_user_id', $1, false), set_config('app.current_org_id', $2, false), set_config('app.system_context', $3, false)`,
		userID,
		orgID,
		systemContext,
	); err != nil {
		return fmt.Errorf("set database session context: %w", err)
	}
	return nil
}
