#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pdf_extraction_result {
    int ok;
    char *text;
    int page_count;
    char *error_message;
} pdf_extraction_result_t;

pdf_extraction_result_t *pdf_extract_text(const char *data, int size);
void pdf_free_result(pdf_extraction_result_t *result);

#ifdef __cplusplus
}
#endif
