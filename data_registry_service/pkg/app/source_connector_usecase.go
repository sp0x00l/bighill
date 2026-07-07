package usecase

import (
	"context"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type SourceUsecase interface {
	CreateSourceConnector(ctx context.Context, sourceConnector *model.SourceConnector, idempotencyKey uuid.UUID) error
	ReadSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) (*model.SourceConnector, error)
	ReplaceSourceConnector(ctx context.Context, sourceConnector *model.SourceConnector) error
	DeleteSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) error
}

type sourceUsecase struct {
	sourceRepository SourceRepositoryAdapter
	unitOfWork       SourceUnitOfWorkAdapter
	catalogClient    CatalogClientAdapter
}

func NewSourceUsecase(sourceRepository SourceRepositoryAdapter, unitOfWork SourceUnitOfWorkAdapter, catalogClient CatalogClientAdapter) *sourceUsecase {
	return &sourceUsecase{
		sourceRepository: sourceRepository,
		unitOfWork:       unitOfWork,
		catalogClient:    catalogClient,
	}
}

func (u *sourceUsecase) CreateSourceConnector(ctx context.Context, sourceConnector *model.SourceConnector, idempotencyKey uuid.UUID) (err error) {
	log.Trace("SourceUsecase CreateSourceConnector")

	attrs := []attribute.KeyValue{attribute.String("idempotency_key", idempotencyKey.String())}
	if sourceConnector != nil {
		attrs = append(attrs, attribute.String("user_id", sourceConnector.UserID.String()))
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "source_connector.create_source_connector", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if sourceConnector != nil {
		orgID, orgErr := requireOrgID(ctx)
		if orgErr != nil {
			return orgErr
		}
		sourceConnector.OrgID = orgID
		ctx = ctxutil.WithActorOrg(ctx, sourceConnector.UserID, sourceConnector.OrgID)
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		id, err := u.sourceRepository.ReserveID(ctx, tx)
		if err != nil {
			return err
		}
		sourceConnector.ID = id
		catalogID, err := u.catalogClient.CreateResource(ctx, id.String(), sourceConnector.Config)
		if err != nil {
			return err
		}
		sourceConnector.CatalogID = catalogID
		if err := u.sourceRepository.Create(ctx, tx, sourceConnector, idempotencyKey); err != nil {
			log.WithContext(ctx).Infof("Rolling back catalog source connector. Failed to create source connector: %v", err)
			err2 := u.catalogClient.DeleteResource(ctx, catalogID)
			if err2 != nil {
				log.WithContext(ctx).Errorf("MANUAL INTERVENTION REQUIRED: Failed to rollback catalog data source creation: %v", err2)
			}
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (u *sourceUsecase) ReadSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) (sourceConnector *model.SourceConnector, err error) {
	log.Trace("SourceUsecase ReadSourceConnector")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "source_connector.read_source_connector",
		attribute.String("connector_id", connectorID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return nil, err
	}
	return u.sourceRepository.ReadByID(ctx, connectorID, userID)
}

func (u *sourceUsecase) ReplaceSourceConnector(ctx context.Context, sourceConnector *model.SourceConnector) (err error) {
	log.Trace("SourceUsecase ReplaceSourceConnector")

	var attrs []attribute.KeyValue
	if sourceConnector != nil {
		attrs = append(attrs,
			attribute.String("connector_id", sourceConnector.ID.String()),
			attribute.String("user_id", sourceConnector.UserID.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "source_connector.replace_source_connector", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if sourceConnector != nil {
		orgID, orgErr := requireOrgID(ctx)
		if orgErr != nil {
			return orgErr
		}
		sourceConnector.OrgID = orgID
		ctx = ctxutil.WithActorOrg(ctx, sourceConnector.UserID, sourceConnector.OrgID)
	}
	originalSourceConn, err := u.sourceRepository.ReadByID(ctx, sourceConnector.ID, sourceConnector.UserID)
	if err != nil {
		return err
	}
	sourceConnector.CatalogID = originalSourceConn.CatalogID

	name := originalSourceConn.ID.String()
	err = u.catalogClient.ReplaceResource(ctx, name, sourceConnector.CatalogID, sourceConnector.Config)
	if err != nil {
		return err
	}

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.sourceRepository.Replace(ctx, tx, sourceConnector)
	})
}

func (u *sourceUsecase) DeleteSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) (err error) {
	log.Trace("SourceUsecase DeleteSourceConnector")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "source_connector.delete_source_connector",
		attribute.String("connector_id", connectorID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return err
	}
	catalogID, err := u.getCatalogID(ctx, connectorID, userID)
	if err != nil {
		return err
	}

	err = u.catalogClient.DeleteResource(ctx, catalogID)
	if err != nil {
		if !domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return err
		}
	}

	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.sourceRepository.Delete(ctx, tx, connectorID, userID)
	})
	if err != nil {
		log.WithContext(ctx).Errorf("Failed to delete source connector: %v", err)
		return err
	}
	return nil

}

func (u *sourceUsecase) getCatalogID(ctx context.Context, connectorID, userID uuid.UUID) (uuid.UUID, error) {
	log.Trace("SourceUsecase getCatalogID")

	return u.sourceRepository.ReadCatalogID(ctx, connectorID, userID)
}
