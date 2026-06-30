package metrics

type Boundary string
type ErrorClass string

const (
	BoundaryDB         Boundary = "db"
	BoundaryKafka      Boundary = "kafka"
	BoundaryGrpcClient Boundary = "grpc_client"
	BoundaryGrpcServer Boundary = "grpc_server"
	BoundaryHTTPClient Boundary = "http_client"
	BoundaryHTTPServer Boundary = "http_server"
	BoundaryRedis      Boundary = "redis"
	BoundaryExternal   Boundary = "external"
)

const (
	ErrorClassTimeout       ErrorClass = "timeout"
	ErrorClassNetwork       ErrorClass = "network"
	ErrorClassUnavailable   ErrorClass = "unavailable"
	ErrorClassCanceled      ErrorClass = "canceled"
	ErrorClassRateLimit     ErrorClass = "rate_limit"
	ErrorClassAuth          ErrorClass = "auth"
	ErrorClassPermission    ErrorClass = "permission"
	ErrorClassNotFound      ErrorClass = "not_found"
	ErrorClassConflict      ErrorClass = "conflict"
	ErrorClassBadResponse   ErrorClass = "bad_response"
	ErrorClassSerialization ErrorClass = "serialization"
	ErrorClassDB            ErrorClass = "db"
	ErrorClassInternal      ErrorClass = "internal"
	ErrorClassUnknown       ErrorClass = "unknown"
)

const (
	labelBoundary   = "boundary"
	labelOperation  = "operation"
	labelErrorClass = "error_class"
	labelStatus     = "status"
)
