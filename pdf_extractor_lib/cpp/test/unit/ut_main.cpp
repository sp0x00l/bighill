#include "bridge/go/cgo_pdf_extractor.h"

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string escape_pdf_text(const std::string &text)
{
    std::string out;
    out.reserve(text.size());
    for (char c : text) {
        if (c == '\\' || c == '(' || c == ')') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

std::string minimal_pdf(const std::string &text)
{
    const std::string stream = "BT /F1 24 Tf 72 720 Td (" + escape_pdf_text(text) + ") Tj ET";
    const std::vector<std::string> objects = {
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        "<< /Length " + std::to_string(stream.size()) + " >>\nstream\n" + stream + "\nendstream",
    };

    std::ostringstream pdf;
    std::vector<std::size_t> offsets = {0};
    pdf << "%PDF-1.4\n";
    for (std::size_t i = 0; i < objects.size(); ++i) {
        offsets.push_back(static_cast<std::size_t>(pdf.tellp()));
        pdf << (i + 1) << " 0 obj\n" << objects[i] << "\nendobj\n";
    }
    const std::size_t xref_offset = static_cast<std::size_t>(pdf.tellp());
    pdf << "xref\n0 " << (objects.size() + 1) << "\n";
    pdf << "0000000000 65535 f \n";
    for (std::size_t i = 1; i < offsets.size(); ++i) {
        pdf.width(10);
        pdf.fill('0');
        pdf << offsets[i] << " 00000 n \n";
    }
    pdf << "trailer\n<< /Root 1 0 R /Size " << (objects.size() + 1) << " >>\n";
    pdf << "startxref\n" << xref_offset << "\n%%EOF\n";
    return pdf.str();
}

void require(bool condition, const std::string &message)
{
    if (!condition) {
        std::cerr << message << std::endl;
        std::exit(1);
    }
}

void test_rejects_empty_input()
{
    pdf_extraction_result_t *result = pdf_extract_text(nullptr, 0);
    require(result != nullptr, "empty input returned nil result");
    require(result->ok == 0, "empty input should fail");
    require(std::string(result->error_message).find("pdf data is required") != std::string::npos,
            "empty input returned unexpected error");
    pdf_free_result(result);
}

void test_extracts_text_from_memory_pdf()
{
    std::string pdf = minimal_pdf("Hello PDF");
    pdf_extraction_result_t *result = pdf_extract_text(pdf.data(), static_cast<int>(pdf.size()));
    require(result != nullptr, "pdf extraction returned nil result");
    require(result->ok == 1, std::string("pdf extraction failed: ") + result->error_message);
    require(result->page_count == 1, "pdf extraction returned wrong page count");
    require(std::string(result->text).find("Hello PDF") != std::string::npos,
            std::string("pdf extraction returned wrong text: ") + result->text);
    pdf_free_result(result);
}

} // namespace

int main()
{
    test_rejects_empty_input();
    test_extracts_text_from_memory_pdf();
    std::cout << "pdf_extractor_lib C++ unit tests passed" << std::endl;
    return 0;
}
