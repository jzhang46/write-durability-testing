#include <mach/vm_param.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

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

    size_t file_size_from_header = *(size_t*)base;
    size_t page_count_from_header = file_size_from_header / PAGE_SIZE;

    // Verify header.
    memset_pattern8(page_buffer, &file_size, PAGE_SIZE);
    if (memcmp(base, page_buffer, PAGE_SIZE)) {
        fprintf(stderr, "Expected first page of file to be filled with 0x%08zx, saw 0x%08zx.\n", file_size, file_size_from_header);
        fprintf(stderr, "Header was from file %zu bytes in size.\n", file_size_from_header);
        if (file_size_from_header > file_size) {
            fprintf(stderr, "The header was updated without all of the body data making it to disk. Corruption!\n");
            return 1;
        } else if (file_size_from_header < PAGE_SIZE) {
            fprintf(stderr, "The header claims the file is empty. Corruption!\n");
            return 1;
        } else {
            fprintf(stderr, "The header is from a smaller file. This is ok assuming the body data is intact.\n");
        }
    }

    // Verify the body.
    for (size_t i = page_count_from_header - 1; i > 0; --i) {
        size_t page_offset = (page_count_from_header - i);
        size_t byte_offset = page_offset * PAGE_SIZE;
        memset_pattern8(page_buffer, &i, PAGE_SIZE);
        if (memcmp(base + byte_offset, page_buffer, PAGE_SIZE)) {
            size_t n = *(size_t*)(base + byte_offset);
            fprintf(stderr, "Expected %zuth page of file to be filled with 0x%08zx, saw 0x%08zx.\n", page_offset, i, n);
            success = false;
        }
    }

    if (success)
        fprintf(stderr, "Verfication succeeded.\n");

    return success;
}
