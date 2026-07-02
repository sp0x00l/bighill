#include "bridge/go/cgo_pdf_extractor.h"

#include <poppler-document.h>
#include <poppler-page.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

char *copy_string(const std::string &value)
{
    auto *out = static_cast<char *>(std::malloc(value.size() + 1));
    if (out == nullptr) {
        throw std::bad_alloc();
    }
    std::memcpy(out, value.c_str(), value.size() + 1);
    return out;
}

pdf_extraction_result_t *new_result()
{
    auto *result = static_cast<pdf_extraction_result_t *>(std::calloc(1, sizeof(pdf_extraction_result_t)));
    if (result == nullptr) {
        throw std::bad_alloc();
    }
    return result;
}

pdf_extraction_result_t *success_result(const std::string &text, int page_count)
{
    auto *result = new_result();
    result->ok = 1;
    result->text = copy_string(text);
    result->page_count = page_count;
    result->error_message = copy_string("");
    return result;
}

pdf_extraction_result_t *error_result(const std::string &message)
{
    auto *result = new_result();
    result->ok = 0;
    result->text = copy_string("");
    result->page_count = 0;
    result->error_message = copy_string(message);
    return result;
}

std::string to_utf8(const poppler::ustring &value)
{
    poppler::byte_array bytes = value.to_utf8();
    return std::string(bytes.begin(), bytes.end());
}

} // namespace

extern "C" pdf_extraction_result_t *pdf_extract_text(const char *data, int size)
{
    try {
        if (data == nullptr || size <= 0) {
            return error_result("pdf data is required");
        }

        std::unique_ptr<poppler::document> document(poppler::document::load_from_raw_data(data, size));
        if (document == nullptr) {
            return error_result("failed to load pdf document");
        }
        if (document->is_locked()) {
            return error_result("pdf document is locked");
        }

        std::ostringstream text;
        const int pages = document->pages();
        for (int index = 0; index < pages; ++index) {
            std::unique_ptr<poppler::page> page(document->create_page(index));
            if (page == nullptr) {
                continue;
            }
            std::string page_text = to_utf8(page->text());
            if (!page_text.empty()) {
                if (text.tellp() > 0) {
                    text << "\n";
                }
                text << page_text;
            }
        }

        return success_result(text.str(), pages);
    } catch (const std::exception &err) {
        try {
            return error_result(err.what());
        } catch (...) {
            return nullptr;
        }
    } catch (...) {
        try {
            return error_result("unknown pdf extraction failure");
        } catch (...) {
            return nullptr;
        }
    }
}

extern "C" void pdf_free_result(pdf_extraction_result_t *result)
{
    if (result == nullptr) {
        return;
    }
    std::free(result->text);
    std::free(result->error_message);
    std::free(result);
}
