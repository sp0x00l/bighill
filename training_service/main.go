package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"training_service/pkg/app"
	"training_service/pkg/domain/model"
	"training_service/pkg/infra/executor"
	trainingmessaging "training_service/pkg/infra/network/messaging"
	"training_service/pkg/infra/temporalworker"

	env "lib/shared_lib/env"
	coreHealthCheck "lib/shared_lib/healthcheck"
	logs "lib/shared_lib/logs"
	messagingConn "lib/shared_lib/messaging"
	trace "lib/shared_lib/trace"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

var Version string

type trainingConfig struct {
	ServiceName            string
	TrainingTriggerEnabled bool
	Temporal               temporalConfig
	Messaging              messagingConn.MessengerConfig
	Topics                 trainingmessaging.TrainingTopics
	BaseModel              string
	EvaluationProfile      string
	DPOEvaluationProfile   string
	PromotionProfile       string
	Profile                model.TrainingProfile
	Executor               trainingExecutorConfig
	Health                 healthConfig
}

type temporalConfig struct {
	Address   string
	Namespace string
	TaskQueue string
}

type healthConfig struct {
	CpuThresholdPercentage                    int
	MemFreeThresholdPercent                   int
	HealthCheckPort                           int
	MessageBrokerConnectionString             string
	ServiceLatencyThreshold                   time.Duration
	MessageBrokerLatencyThreshold             time.Duration
	MessageBrokerSubscriberMaxPollSilence     time.Duration
	MessageBrokerSubscriberMaxProgressSilence time.Duration
	MessageBrokerSubscriberMaxLag             int64
}

type trainingExecutorConfig struct {
	Provider                 string
	RayJobsURL               string
	RayTrainingEntrypoint    string
	RayEvaluationEntrypoint  string
	RayPromotionEntrypoint   string
	AxolotlCommand           string
	RayRequestTimeout        time.Duration
	RayPollInterval          time.Duration
	KubeRayNamespace         string
	KubeRayRayVersion        string
	KubeRayImage             string
	KubeRayImagePullPolicy   string
	KubeRayServiceAccount    string
	KubeRayTTLSeconds        int
	KubeRayWorkerReplicas    int
	KubeRayCPU               string
	KubeRayMemory            string
	KubeRayGPUResource       string
	KubeRayGPU               string
	ArtifactBucketRegion     string
	ModelURIPrefix           string
	EvaluationURIPrefix      string
	PromotionReportURIPrefix string
	ServingTarget            string
	ServingModel             string
	ServingLoadStatus        string
}

func init() {
	logs.Init()
}

func main() {
	ctx := context.Background()
	cancelCtx, cancelFtn := context.WithCancel(ctx)
	defer cancelFtn()

	cfg := readTrainingConfig()
	serviceName := cfg.ServiceName

	log.Trace(fmt.Sprintf("starting the %s service", serviceName))
	traceShutdown := trace.Init(cancelCtx, serviceName, Version)
	defer traceShutdown()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to connect to Temporal")
	}
	defer temporalClient.Close()

	publisherFactory := messagingConn.NewMessenger(cfg.Messaging, cancelFtn)
	defer func() {
		_ = publisherFactory.Close(cancelCtx)
	}()
	publisher, err := publisherFactory.Publisher(cancelCtx)
	if err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to create training publisher")
	}
	subscriberFactories := []messagingConn.Messenger{}
	defer func() {
		for _, factory := range subscriberFactories {
			_ = factory.Close(cancelCtx)
		}
	}()

	trainingEventPublisher := trainingmessaging.NewTrainingEventPublisher(publisher, cfg.Topics)
	activityOptions := []temporalworker.TrainingActivitiesOption{
		temporalworker.WithModelURIPrefix(cfg.Executor.ModelURIPrefix),
		temporalworker.WithEvaluationURIPrefix(cfg.Executor.EvaluationURIPrefix),
		temporalworker.WithServingConfig(cfg.Executor.ServingTarget, cfg.Executor.ServingModel, cfg.Executor.ServingLoadStatus),
		temporalworker.WithArtifactBucketRegion(cfg.Executor.ArtifactBucketRegion),
		temporalworker.WithAxolotlCommand(cfg.Executor.AxolotlCommand),
	}
	var trainingExecutor app.TrainingExecutor
	if cfg.TrainingTriggerEnabled {
		manifestReader, err := executor.NewObjectManifestReader(cancelCtx, cfg.Executor.ArtifactBucketRegion, nil)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create training manifest reader")
		}
		trainingExecutor, err = newTrainingExecutor(cfg.Executor, manifestReader)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatal("unable to create training executor")
		}
		activityOptions = append(activityOptions, temporalworker.WithExecutor(trainingExecutor))
	}
	activities := temporalworker.NewTrainingActivities(trainingEventPublisher, activityOptions...)
	trainingWorker := temporalworker.NewTrainingWorker(temporalClient, cfg.Temporal.TaskQueue, activities)
	if err := trainingWorker.Start(); err != nil {
		log.WithContext(cancelCtx).WithError(err).Fatal("unable to start Temporal worker")
	}
	defer trainingWorker.Stop()

	healthCheck := coreHealthCheck.NewMonitor(newHealthCheckConfig(cfg.Health))
	healthCheck = healthCheck.WithCpuCheck().WithMemoryCheck().WithMessageBrokerCheck()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	startSubscriber := func(name string, topics []string, configure func(messagingConn.Subscriber)) {
		factory, monitor, err := messagingConn.StartStreamSubscriber(cancelCtx, messagingConn.StreamSubscriberConfig{
			Brokers:          cfg.Messaging.Brokers,
			DLQURL:           cfg.Messaging.DlqURL,
			BaseGroupID:      cfg.Messaging.GroupID,
			AutoOffsetReset:  cfg.Messaging.AutoOffsetReset,
			Cancel:           cancelFtn,
			Monitor:          healthCheck,
			OnUnexpectedStop: func() { quit <- syscall.SIGTERM },
		}, name, topics, configure)
		if err != nil {
			log.WithContext(cancelCtx).WithError(err).Fatalf("unable to create %s subscriber", name)
		}
		healthCheck = monitor
		subscriberFactories = append(subscriberFactories, factory)
	}

	if cfg.TrainingTriggerEnabled {
		workflowStarter := temporalworker.NewTrainingWorkflowStarter(temporalClient, cfg.Temporal.TaskQueue)
		startSubscriber("dataset-updated", []string{cfg.Topics.DataRegistry}, func(subscriber messagingConn.Subscriber) {
			messagingConn.AddListener(subscriber, trainingmessaging.NewDatasetUpdatedEventListener(workflowStarter, cfg.BaseModel, cfg.Profile, cfg.EvaluationProfile))
		})
		startSubscriber("preference-dataset-ready", []string{cfg.Topics.Inference}, func(subscriber messagingConn.Subscriber) {
			messagingConn.AddListener(subscriber, trainingmessaging.NewPreferenceDatasetReadyEventListener(workflowStarter, cfg.BaseModel, cfg.Profile, cfg.DPOEvaluationProfile))
		})
		promotionRunner := trainingmessaging.NewPromotionReportRunner(trainingExecutor, trainingEventPublisher, cfg.Executor.PromotionReportURIPrefix, cfg.Executor.ArtifactBucketRegion, cfg.PromotionProfile)
		startSubscriber("promotion-requested", []string{cfg.Topics.ModelRegistry}, func(subscriber messagingConn.Subscriber) {
			messagingConn.AddListener(subscriber, trainingmessaging.NewPromotionRequestedEventListener(promotionRunner))
		})
	} else {
		log.WithContext(cancelCtx).Info("training trigger disabled; dataset materialization events will not start training workflows")
	}

	go func() {
		if err := healthCheck.Connect(cancelCtx); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("unable to start health check for the %s service: %v", serviceName, err)
			}
			quit <- syscall.SIGTERM
		}
	}()

	log.WithContext(cancelCtx).WithFields(log.Fields{
		"temporal_address":    cfg.Temporal.Address,
		"temporal_namespace":  cfg.Temporal.Namespace,
		"temporal_task_queue": cfg.Temporal.TaskQueue,
		"workflow":            app.TrainModelWorkflowName,
	}).Info("training Temporal worker started")

	<-quit

	cancelFtn()
	healthCheck.Close()
	log.Trace(fmt.Sprintf("stopped the %s service", serviceName))
}

func readTrainingConfig() trainingConfig {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	return trainingConfig{
		ServiceName:            env.WithDefaultString("TRAINING_SERVICE_NAME", "training-service"),
		TrainingTriggerEnabled: env.WithDefaultBool("TRAINING_SERVICE_TRAINING_TRIGGER_ENABLED", false),
		Temporal: temporalConfig{
			Address:   env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_ADDRESS", env.WithDefaultString("TEMPORAL_ADDRESS", "localhost:7233")),
			Namespace: env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_NAMESPACE", env.WithDefaultString("TEMPORAL_NAMESPACE", "default")),
			TaskQueue: env.WithDefaultString("TRAINING_SERVICE_TEMPORAL_TASK_QUEUE", app.DefaultTrainingWorkflowTaskQueue),
		},
		Messaging: messagingConn.MessengerConfig{
			DlqURL:          env.WithDefaultString("TRAINING_SERVICE_DLQ", "http://localhost:4566/training-dev-env-queue/"),
			GroupID:         env.WithDefaultString("TRAINING_SERVICE_KAFKA_BASE_GROUP_ID", "training"),
			Brokers:         brokers,
			AutoOffsetReset: env.WithDefaultString("TRAINING_SERVICE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
		},
		Topics: trainingmessaging.TrainingTopics{
			DataRegistry:  env.WithDefaultString("TRAINING_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC", "data_registry"),
			Inference:     env.WithDefaultString("TRAINING_SERVICE_INFERENCE_SUBSCRIBER_TOPIC", "inference"),
			ModelRegistry: env.WithDefaultString("TRAINING_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC", "model_registry"),
			Training:      env.WithDefaultString("TRAINING_SERVICE_TOPIC", "training"),
		},
		BaseModel:         env.WithDefaultString("TRAINING_SERVICE_BASE_MODEL", "local-dev-base-model"),
		EvaluationProfile: env.WithDefaultString("TRAINING_SERVICE_EVALUATION_PROFILE", "smoke"),
		DPOEvaluationProfile: env.WithDefaultString(
			"TRAINING_SERVICE_DPO_EVALUATION_PROFILE",
			`{"metric_suite":"preference","evaluator_name":"pairwise-judge","evaluator_version":"v1"}`,
		),
		PromotionProfile: env.WithDefaultString("TRAINING_SERVICE_PROMOTION_PROFILE", "{}"),
		Profile: model.TrainingProfile{
			Name:                      env.WithDefaultString("TRAINING_SERVICE_TRAINING_PROFILE_NAME", "local-dev-qlora"),
			Trainer:                   env.WithDefaultString("TRAINING_SERVICE_TRAINING_PROFILE_TRAINER", "sft"),
			Adapter:                   env.WithDefaultString("TRAINING_SERVICE_TRAINING_PROFILE_ADAPTER", "qlora"),
			Quantization:              env.WithDefaultString("TRAINING_SERVICE_TRAINING_PROFILE_QUANTIZATION", "4bit"),
			PreferenceDatasetURI:      env.WithDefaultString("TRAINING_SERVICE_TRAINING_PROFILE_PREFERENCE_DATASET_URI", ""),
			DPOBeta:                   floatFromEnv("TRAINING_SERVICE_TRAINING_PROFILE_DPO_BETA", "0.1"),
			SequenceLength:            env.WithDefaultInt("TRAINING_SERVICE_TRAINING_PROFILE_SEQUENCE_LENGTH", "2048"),
			SamplePacking:             env.WithDefaultBool("TRAINING_SERVICE_TRAINING_PROFILE_SAMPLE_PACKING", true),
			LearningRate:              floatFromEnv("TRAINING_SERVICE_TRAINING_PROFILE_LEARNING_RATE", "0.0002"),
			Epochs:                    floatFromEnv("TRAINING_SERVICE_TRAINING_PROFILE_EPOCHS", "3"),
			MicroBatchSize:            env.WithDefaultInt("TRAINING_SERVICE_TRAINING_PROFILE_MICRO_BATCH_SIZE", "1"),
			GradientAccumulationSteps: env.WithDefaultInt("TRAINING_SERVICE_TRAINING_PROFILE_GRADIENT_ACCUMULATION_STEPS", "4"),
			LoRAR:                     env.WithDefaultInt("TRAINING_SERVICE_TRAINING_PROFILE_LORA_R", "16"),
			LoRAAlpha:                 env.WithDefaultInt("TRAINING_SERVICE_TRAINING_PROFILE_LORA_ALPHA", "32"),
		},
		Executor: trainingExecutorConfig{
			Provider:                 env.WithDefaultString("TRAINING_SERVICE_EXECUTOR_PROVIDER", "kuberay"),
			RayJobsURL:               env.WithDefaultString("TRAINING_SERVICE_RAY_JOBS_URL", "http://localhost:8265"),
			RayTrainingEntrypoint:    env.WithDefaultString("TRAINING_SERVICE_RAY_TRAINING_ENTRYPOINT", "python -m training_jobs.train"),
			RayEvaluationEntrypoint:  env.WithDefaultString("TRAINING_SERVICE_RAY_EVALUATION_ENTRYPOINT", "python -m training_jobs.evaluate"),
			RayPromotionEntrypoint:   env.WithDefaultString("TRAINING_SERVICE_RAY_PROMOTION_ENTRYPOINT", "python -m training_jobs.promotion_report"),
			AxolotlCommand:           env.WithDefaultString("TRAINING_SERVICE_AXOLOTL_COMMAND", "axolotl train"),
			RayRequestTimeout:        secondsFromEnv("TRAINING_SERVICE_RAY_REQUEST_TIMEOUT_SECONDS", "30"),
			RayPollInterval:          secondsFromEnv("TRAINING_SERVICE_RAY_POLL_INTERVAL_SECONDS", "30"),
			KubeRayNamespace:         env.WithDefaultString("TRAINING_SERVICE_KUBERAY_NAMESPACE", "default"),
			KubeRayRayVersion:        env.WithDefaultString("TRAINING_SERVICE_KUBERAY_RAY_VERSION", "2.46.0"),
			KubeRayImage:             env.WithDefaultString("TRAINING_SERVICE_KUBERAY_IMAGE", "training-jobs:0.0.1"),
			KubeRayImagePullPolicy:   env.WithDefaultString("TRAINING_SERVICE_KUBERAY_IMAGE_PULL_POLICY", "IfNotPresent"),
			KubeRayServiceAccount:    env.WithDefaultString("TRAINING_SERVICE_KUBERAY_SERVICE_ACCOUNT", "training-jobs"),
			KubeRayTTLSeconds:        env.WithDefaultInt("TRAINING_SERVICE_KUBERAY_TTL_SECONDS_AFTER_FINISHED", "3600"),
			KubeRayWorkerReplicas:    env.WithDefaultInt("TRAINING_SERVICE_KUBERAY_WORKER_REPLICAS", "1"),
			KubeRayCPU:               env.WithDefaultString("TRAINING_SERVICE_KUBERAY_CPU", "1"),
			KubeRayMemory:            env.WithDefaultString("TRAINING_SERVICE_KUBERAY_MEMORY", "4Gi"),
			KubeRayGPUResource:       env.WithDefaultString("TRAINING_SERVICE_KUBERAY_GPU_RESOURCE", "nvidia.com/gpu"),
			KubeRayGPU:               env.WithDefaultString("TRAINING_SERVICE_KUBERAY_GPU", "1"),
			ArtifactBucketRegion:     env.WithDefaultString("TRAINING_SERVICE_ARTIFACT_BUCKET_REGION", "eu-west-1"),
			ModelURIPrefix:           env.WithDefaultString("TRAINING_SERVICE_MODEL_URI_PREFIX", "s3://local-dev-bucket/models"),
			EvaluationURIPrefix:      env.WithDefaultString("TRAINING_SERVICE_EVALUATION_URI_PREFIX", "s3://local-dev-bucket/evaluations"),
			PromotionReportURIPrefix: env.WithDefaultString("TRAINING_SERVICE_PROMOTION_REPORT_URI_PREFIX", "s3://local-dev-bucket/promotion-reports"),
			ServingTarget:            env.WithDefaultString("TRAINING_SERVICE_SERVING_TARGET", ""),
			ServingModel:             env.WithDefaultString("TRAINING_SERVICE_SERVING_MODEL", ""),
			ServingLoadStatus:        env.WithDefaultString("TRAINING_SERVICE_SERVING_LOAD_STATUS", "NOT_LOADED"),
		},
		Health: healthConfig{
			CpuThresholdPercentage:                    env.WithDefaultInt("TRAINING_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT", "80"),
			MemFreeThresholdPercent:                   env.WithDefaultInt("TRAINING_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT", "20"),
			HealthCheckPort:                           env.WithDefaultInt("TRAINING_SERVICE_HEALTHCHECK_PORT", "5058"),
			MessageBrokerConnectionString:             brokers,
			ServiceLatencyThreshold:                   secondsFromEnv("TRAINING_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerLatencyThreshold:             secondsFromEnv("TRAINING_SERVICE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS", "5"),
			MessageBrokerSubscriberMaxPollSilence:     secondsFromEnv("TRAINING_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_POLL_SILENCE_SECONDS", "30"),
			MessageBrokerSubscriberMaxProgressSilence: secondsFromEnv("TRAINING_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_PROGRESS_SILENCE_SECONDS", "90"),
			MessageBrokerSubscriberMaxLag:             int64(env.WithDefaultInt("TRAINING_SERVICE_HEALTHCHECK_MESSAGE_BROKER_SUBSCRIBER_MAX_LAG", "100000")),
		},
	}
}

func newTrainingExecutor(cfg trainingExecutorConfig, manifestReader executor.ManifestReader) (app.TrainingExecutor, error) {
	log.Trace("newTrainingExecutor")

	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "ray":
		if cfg.RayPollInterval >= app.DefaultTrainingActivityHeartbeat {
			return nil, fmt.Errorf("ray poll interval must be less than training activity heartbeat timeout")
		}
		return executor.NewRayExecutor(executor.RayExecutorConfig{
			URL:                  cfg.RayJobsURL,
			TrainingEntrypoint:   cfg.RayTrainingEntrypoint,
			EvaluationEntrypoint: cfg.RayEvaluationEntrypoint,
			PromotionEntrypoint:  cfg.RayPromotionEntrypoint,
			RequestTimeout:       cfg.RayRequestTimeout,
			PollInterval:         cfg.RayPollInterval,
		}, manifestReader)
	case "kuberay":
		if cfg.RayPollInterval >= app.DefaultTrainingActivityHeartbeat {
			return nil, fmt.Errorf("kuberay poll interval must be less than training activity heartbeat timeout")
		}
		return executor.NewKubeRayExecutor(executor.KubeRayExecutorConfig{
			Namespace:               cfg.KubeRayNamespace,
			RayVersion:              cfg.KubeRayRayVersion,
			Image:                   cfg.KubeRayImage,
			ImagePullPolicy:         cfg.KubeRayImagePullPolicy,
			ServiceAccountName:      cfg.KubeRayServiceAccount,
			TTLSecondsAfterFinished: cfg.KubeRayTTLSeconds,
			WorkerReplicas:          cfg.KubeRayWorkerReplicas,
			CPU:                     cfg.KubeRayCPU,
			Memory:                  cfg.KubeRayMemory,
			GPUResource:             cfg.KubeRayGPUResource,
			GPU:                     cfg.KubeRayGPU,
			TrainingEntrypoint:      cfg.RayTrainingEntrypoint,
			EvaluationEntrypoint:    cfg.RayEvaluationEntrypoint,
			PromotionEntrypoint:     cfg.RayPromotionEntrypoint,
			PollInterval:            cfg.RayPollInterval,
		}, manifestReader)
	default:
		return nil, fmt.Errorf("unsupported training executor provider %q", cfg.Provider)
	}
}

func newHealthCheckConfig(cfg healthConfig) coreHealthCheck.HealthCheckConfig {
	return coreHealthCheck.HealthCheckConfig{
		CpuThresholdPercentage:                       cfg.CpuThresholdPercentage,
		MemFreeThresholdPercentage:                   cfg.MemFreeThresholdPercent,
		HealthCheckPort:                              cfg.HealthCheckPort,
		DBConnectionString:                           "",
		MessageBrokerConnectionString:                cfg.MessageBrokerConnectionString,
		DbLatencyThresholdSec:                        0,
		MessageBrokerLatencyThresholdSec:             cfg.MessageBrokerLatencyThreshold,
		ServiceLatencyThresholdSec:                   cfg.ServiceLatencyThreshold,
		HttpCheckTargets:                             map[string]string{},
		MessageBrokerSubscriberMaxPollSilenceSec:     cfg.MessageBrokerSubscriberMaxPollSilence,
		MessageBrokerSubscriberMaxProgressSilenceSec: cfg.MessageBrokerSubscriberMaxProgressSilence,
		MessageBrokerSubscriberMaxLag:                cfg.MessageBrokerSubscriberMaxLag,
	}
}

func secondsFromEnv(key, defaultValue string) time.Duration {
	return time.Duration(env.WithDefaultInt(key, defaultValue)) * time.Second
}

func floatFromEnv(key, defaultValue string) float64 {
	value := env.WithDefaultString(key, defaultValue)
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Fatalf("could not load environment variable %s=%q, expected float: %v", key, value, err)
	}
	return parsed
}
