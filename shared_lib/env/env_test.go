package config_test

import (
	"errors"
	config "lib/shared_lib/env"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInfraEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "env config unit test suite")
}

func TestFatalEnvHelper(t *testing.T) {
	switch os.Getenv("SHARED_LIB_ENV_FATAL_CASE") {
	case "invalid_int64":
		os.Setenv("ENV_VAT_INT64", "*")
		config.WithDefaultInt64("ENV_VAT_INT64", "0")
	case "invalid_int":
		os.Setenv("ENV_VAT_INT", "*")
		config.WithDefaultInt("ENV_VAT_INT", "8080")
	case "invalid_duration":
		os.Setenv("ENV_VAT_DURATION", "sixty")
		config.MustDuration("ENV_VAT_DURATION")
	case "empty_string_slice":
		os.Setenv("ENV_VAT_STR_ARRAY", "")
		config.WithDefaultStringSlice("ENV_VAT_STR_ARRAY", "default-value1,default-value2")
	case "empty_string_slice_item":
		os.Setenv("ENV_VAT_STR_ARRAY", "value1,")
		config.WithDefaultStringSlice("ENV_VAT_STR_ARRAY", "default-value1,default-value2")
	case "empty_default_string_slice":
		os.Unsetenv("ENV_VAT_STR_ARRAY")
		config.WithDefaultStringSlice("ENV_VAT_STR_ARRAY", "")
	default:
		t.Skip("helper process")
	}
}

var _ = Describe("environment variables", func() {
	Describe("Reading int64 env variables", func() {
		int64Val := rand.Int64()
		Context("when the environment variable is a valid int64 number", func() {
			BeforeEach(func() {
				int64Str := strconv.FormatInt(int64Val, 10)
				os.Setenv("ENV_VAT_INT64", int64Str)
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_INT64")
			})
			It("return the env var as a int64", func() {
				result := config.WithDefaultInt64("ENV_VAT_INT64", "0")
				Expect(result).Should(Equal(int64Val))
			})
		})

		Context("when the environment variable is an invalid int64 number", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_INT64", "*")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_INT64")
			})

			It("exits fatally", func() {
				expectFatalEnvCall("invalid_int64")
			})
		})

		Context("when the environment variable is empty", func() {
			BeforeEach(func() {
				os.Unsetenv("ENV_VAT_INT64")
			})

			It("returns the default value as an int64", func() {
				int64Default := rand.Int64()
				int64DefaultStr := strconv.FormatInt(int64Default, 10)
				result := config.WithDefaultInt64("ENV_VAT_INT64", int64DefaultStr)
				Expect(result).Should(Equal(int64Default))
			})
		})
	})

	Describe("Reading int env variables", func() {
		intVal := rand.Int()
		Context("when the environment variable is a valid int number", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_INT", strconv.Itoa(intVal))
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_INT")
			})
			It("returns the env variable value as an int", func() {
				result := config.WithDefaultInt("ENV_VAT_INT", "0")
				Expect(result).Should(Equal(intVal))
			})
		})

		Context("when the environment variable is not a number", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_INT", "*")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_INT")
			})

			It("exits fatally", func() {
				expectFatalEnvCall("invalid_int")
			})
		})

		Context("when the environment variable is empty", func() {
			BeforeEach(func() {
				os.Unsetenv("ENV_VAT_INT")
			})

			It("returns the default value", func() {
				result := config.WithDefaultInt("ENV_VAT_INT", "1")
				Expect(result).Should(Equal(1))
			})
		})
	})

	Describe("Reading duration env variables", func() {
		Context("when the environment variable is a valid duration", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_DURATION", "60s")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_DURATION")
			})

			It("returns the env variable value as a duration", func() {
				result := config.MustDuration("ENV_VAT_DURATION")
				Expect(result).Should(Equal(time.Minute))
			})
		})

		Context("when the environment variable is not a duration", func() {
			It("exits fatally", func() {
				expectFatalEnvCall("invalid_duration")
			})
		})
	})

	Describe("Reading string env variables", func() {
		Context("when the string variable is a valid environment", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_STR", "expected-string")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_STR")
			})
			It("return the env variable value as a string", func() {
				result := config.WithDefaultString("ENV_VAT_STR", "default-string")
				Expect(result).Should(Equal("expected-string"))
			})
		})

		Context("when the string environment variable is not set", func() {
			It("return the default variable value as a string", func() {
				result := config.WithDefaultString("ENV_VAT_STR", "default-string")
				Expect(result).Should(Equal("default-string"))
			})
		})
	})

	Describe("Reading string arrays from env variables", func() {
		Context("when the environment variable is a valid string array", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_STR_ARRAY", "value1,value2,value3")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_STR_ARRAY")
			})
			It("returns the env variable value as a string array", func() {
				result := config.WithDefaultStringSlice("ENV_VAT_STR_ARRAY", "default-value1,default-value2")
				Expect(len(result)).Should(Equal(3))
				Expect(result).Should(ContainElements("value1", "value2", "value3"))
			})
		})

		Context("when the environment variable is not set", func() {
			It("returns the default value as a string array", func() {
				result := config.WithDefaultStringSlice("ENV_VAT_STR_ARRAY", "default-value1,default-value2")
				Expect(len(result)).Should(Equal(2))
				Expect(result).Should(ContainElements("default-value1", "default-value2"))
			})
		})

		Context("when the environment variable is explicitly empty", func() {
			It("fails loudly", func() {
				expectFatalEnvCall("empty_string_slice")
			})
		})

		Context("when the environment variable contains an empty item", func() {
			It("fails loudly", func() {
				expectFatalEnvCall("empty_string_slice_item")
			})
		})

		Context("when the default value is empty", func() {
			It("fails loudly", func() {
				expectFatalEnvCall("empty_default_string_slice")
			})
		})
	})

	Describe("Reading map from env variables", func() {
		Context("when the environment variable is a valid map", func() {
			BeforeEach(func() {
				os.Setenv("ENV_VAT_MAP", "key1:100,key2:200,key3:300")
			})
			AfterEach(func() {
				os.Unsetenv("ENV_VAT_MAP")
			})
			It("returns the env variable value as a map", func() {
				result := config.WithDefaultMap("ENV_VAT_MAP", "defaultKey1:10,defaultKey2:20")
				Expect(len(result)).Should(Equal(3))
				Expect(result).Should(HaveKeyWithValue("key1", int64(100)))
				Expect(result).Should(HaveKeyWithValue("key2", int64(200)))
				Expect(result).Should(HaveKeyWithValue("key3", int64(300)))
			})
		})

		Context("when the environment variable is not set", func() {
			It("returns the default value as a map", func() {
				result := config.WithDefaultMap("ENV_VAT_MAP", "defaultKey1:10,defaultKey2:20")
				Expect(len(result)).Should(Equal(2))
				Expect(result).Should(HaveKeyWithValue("defaultKey1", int64(10)))
				Expect(result).Should(HaveKeyWithValue("defaultKey2", int64(20)))
			})
		})
	})
})

func expectFatalEnvCall(caseName string) {
	cmd := exec.Command(os.Args[0], "-test.run=TestFatalEnvHelper")
	cmd.Env = append(os.Environ(), "SHARED_LIB_ENV_FATAL_CASE="+caseName)
	err := cmd.Run()
	Expect(err).To(HaveOccurred())

	var exitErr *exec.ExitError
	Expect(errors.As(err, &exitErr)).To(BeTrue())
	Expect(exitErr.ExitCode()).NotTo(Equal(0))
}
