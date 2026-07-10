package materialization

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Materialization internals", func() {
	Describe("flight TLS config", func() {
		It("skips TLS config in insecure mode", func() {
			cfg, err := flightTLSConfig(FlightDataStreamReaderConfig{Insecure: true})

			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).To(BeNil())
		})

		It("sets the configured server name", func() {
			cfg, err := flightTLSConfig(FlightDataStreamReaderConfig{ServerName: "data-stream.internal"})

			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.ServerName).To(Equal("data-stream.internal"))
		})

		It("requires client cert and key together", func() {
			_, err := flightTLSConfig(FlightDataStreamReaderConfig{ClientCertPath: "client.crt"})

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("client cert and key must be configured together")))
		})

		It("rejects missing CA cert files", func() {
			_, err := flightTLSConfig(FlightDataStreamReaderConfig{CACertPath: "/does/not/exist/ca.pem"})

			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("read data stream ca cert")))
		})
	})

	Describe("document helpers", func() {
		It("builds paragraph sections from plain text", func() {
			sections := plainDocumentSections("First paragraph\n\nSecond paragraph")

			Expect(sections).To(HaveLen(2))
			Expect(sections[0].Kind).To(Equal(documentSectionKindParagraph))
			Expect(sections[1].Kind).To(Equal(documentSectionKindParagraph))
		})

		It("classifies supported and unsupported HTML section tags", func() {
			kind, level, ok := htmlSectionKind("H6")
			Expect(ok).To(BeTrue())
			Expect(kind).To(Equal(documentSectionKindHeading))
			Expect(level).To(Equal(6))

			kind, level, ok = htmlSectionKind("article")
			Expect(ok).To(BeFalse())
			Expect(kind).To(BeEmpty())
			Expect(level).To(BeZero())
		})
	})

	Describe("parquet helpers", func() {
		It("writes string tables with schema metadata and readable rows", func() {
			data, metadata, err := writeStringTableParquet(
				[]string{"name", "count", "enabled"},
				[]map[string]string{{"name": "movie", "count": "2", "enabled": "true"}},
			)

			Expect(err).NotTo(HaveOccurred())
			Expect(data).NotTo(BeEmpty())
			Expect(metadata).To(ContainSubstring(`"rows":1`))

			rows, err := ExtractTextRowsFromParquet(context.Background(), data, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows).To(HaveLen(1))
			Expect(rows[0]).To(ContainSubstring("movie"))
		})
	})

	Describe("flight reader construction", func() {
		It("stores address and timeout", func() {
			reader := NewFlightDataStreamReader("localhost:7072", time.Second)

			Expect(reader.address).To(Equal("localhost:7072"))
			Expect(reader.timeout).To(Equal(time.Second))
		})

		It("selects insecure transport credentials when configured", func() {
			reader := NewFlightDataStreamReaderWithConfig(FlightDataStreamReaderConfig{
				Address:  "localhost:7072",
				Timeout:  time.Second,
				Insecure: true,
			})

			Expect(reader.transportCredentials().Info().SecurityProtocol).To(Equal("insecure"))
		})

		It("selects TLS transport credentials by default", func() {
			reader := NewFlightDataStreamReaderWithConfig(FlightDataStreamReaderConfig{
				Address:    "localhost:7072",
				Timeout:    time.Second,
				ServerName: "data-stream.internal",
			})

			Expect(reader.transportCredentials().Info().SecurityProtocol).To(Equal("tls"))
		})
	})
})
