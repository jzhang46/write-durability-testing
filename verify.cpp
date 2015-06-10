#include <mach/vm_param.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct page_entry { size_t index, version; };

int main(int argc, char** argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: verify [filename]\n");
        return 1;
    }

    std::string file_name = argv[1];
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd == -1) {
        perror("open");
        return 1;
    }

    struct stat st;
    if (fstat(fd, &st)) {
        perror("fstat");
        return 1;
    }

    size_t file_size = st.st_size;
    fprintf(stderr, "File is %zu bytes in size.\n", file_size);

    char* base = (char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base == (char*)-1) {
        perror("mmap");
        return 1;
    }

    bool success = true;
    char page_buffer[PAGE_SIZE];

    struct header_entry { size_t offset, index, version, marker; };
    header_entry* header_entries = (header_entry*)base;
    for (size_t i = 0; i < 16; ++i) {
        header_entry *header = &header_entries[i];
        if (header->marker != std::numeric_limits<size_t>::max()) {
            fprintf(stderr, "%2zu: %zu %zu %zu 0x%016zx\n", i, header->offset, header->index, header->version, header->marker);
            fprintf(stderr, "    Not a valid header entry. Skipping.\n\n");
            continue;
        }

        size_t byte_offset = header->offset + header->index * PAGE_SIZE;
        if (!i)
            fprintf(stderr, "File data expected to start at byte offset %zu.\n\n", byte_offset);

        if (byte_offset > file_size - PAGE_SIZE) {
            fprintf(stderr, "%2zu: %zu %zu %zu 0x%016zx\n", i, header->offset, header->index, header->version, header->marker);
            fprintf(stderr, "    Byte offset in header entry (%zu) is large than file size!\n\n", byte_offset);
            success = false;
            continue;
        }

        page_entry actual_entry = *(page_entry*)(base + byte_offset);
        fprintf(stderr, "%2zu: { 0x%016zx, 0x%016zx }\n", i, header->index, header->version);
        fprintf(stderr, "%2s  { 0x%016zx, 0x%016zx }", "", actual_entry.index, actual_entry.version);

        page_entry pattern = { header->index, header->version };
        memset_pattern16(page_buffer, &pattern, PAGE_SIZE);
        if (memcmp(base + byte_offset, page_buffer, PAGE_SIZE)) {
            page_entry next_pattern = { header->index, header->version + 1 };
            memset_pattern16(page_buffer, &next_pattern, PAGE_SIZE);
            if (!memcmp(base + byte_offset, page_buffer, PAGE_SIZE)) {
                fprintf(stderr, " - data is a newer version than header entry. Writer was interrupted after writing data and before updating header entry?");
            } else {
                fprintf(stderr, " - expected { 0x%016zx, 0x%016zx }!", pattern.index, pattern.version);
                success = false;
            }
        }
        fprintf(stderr, "\n\n");
    }

    if (success)
        fprintf(stderr, "Verfication succeeded.\n");

    return success;
}
