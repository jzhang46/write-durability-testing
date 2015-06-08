#include <assert.h>
#include <chrono>
#include <dirent.h>
#include <fcntl.h>
#include <random>
#include <stdio.h>
#include <string>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

enum class SyncOperation {
    msync,
    fsync,
    fullfsync,
};

enum class WriteOperation {
    mmap,
    write,
};

std::vector<SyncOperation> sync_operations;
WriteOperation write_operation;

std::string current_timestamp()
{
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d-%H-%M-%S", &tstruct);
    return buf;
}

int write_data(int fd, void* base, off_t offset, void* data, size_t length)
{
    switch (write_operation) {
    case WriteOperation::mmap:
        memcpy(static_cast<char*>(base) + offset, data, length);
        return length;
    case WriteOperation::write:
        return pwrite(fd, data, length, offset);
    }
}

int sync_data(int fd, void* base, size_t size)
{
    for (auto operation : sync_operations) {
        switch (operation) {
        case SyncOperation::msync: {
            if (base && msync(base, size, MS_SYNC)) {
                perror("msync");
                return 1;
            }
            break;
        }
        
        case SyncOperation::fsync: {
            if (fsync(fd)) {
                perror("fsync");
                return 1;
            }
            break;
        }
        
        case SyncOperation::fullfsync: {
            if (fcntl(fd, F_FULLFSYNC)) {
                perror("fcntl");
                return 1;
            }
            break;
        }
        default:
            assert(false);
        }
    }

    return 0;
}

int initialize_from_arguments(int argc, char** argv)
{
    if (argc != 3)
        return 1;

    char* sync_operation_list_string = argv[1];
    char* context;
    for (const char* operation_cstr = strtok_r(sync_operation_list_string, ",", &context);
         operation_cstr;
         operation_cstr = strtok_r(nullptr, ",", &context)) {
        std::string operation { operation_cstr };
        if (operation == "msync")
            sync_operations.push_back(SyncOperation::msync);
        else if (operation == "fsync")
            sync_operations.push_back(SyncOperation::fsync);
        else if (operation == "fullfsync")
            sync_operations.push_back(SyncOperation::fullfsync);
        else
            return 1;
    }

    std::string write_operation_string = argv[2];
    if (write_operation_string == "mmap")
        write_operation = WriteOperation::mmap;
    else if (write_operation_string == "write")
        write_operation = WriteOperation::write;
    else
        return 1;

    return 0;
}

int main(int argc, char** argv)
{
    if (initialize_from_arguments(argc, argv)) {
        fprintf(stderr, "Usage: main msync,fsync,fullfsync [mmap|write]\n");
        return 1;
    }

    std::string working_directory = "working";

    if (mkdir(working_directory.c_str(), 0777) && errno != EEXIST) {
        perror("mkdir");
        return 1;
    }

    DIR *directory = opendir(working_directory.c_str());
    if (!directory) {
        perror("opendir");
        return 1;
    }

    std::string test_file_name = working_directory + "/test-" + current_timestamp() + ".dat";
    fprintf(stderr, "Test file: %s\n", test_file_name.c_str());

    int fd = open(test_file_name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
    if (fd == -1) {
        perror("open");
        return 1;
    }

    int directory_fd = dirfd(directory);
    if (directory_fd == -1) {
        perror("dirfd");
        return 1;
    }

    if (fsync(directory_fd)) {
        perror("fsync");
        return 1;
    }

    closedir(directory);

    for (size_t i = 0; i < 1024; ++i) {
        size_t page_count = 10 + i * 10;
        size_t file_size = page_count * PAGE_SIZE;
        if (i > 0)
            fputc('\n', stderr);
        fprintf(stderr, "Truncating file to %zu bytes.\n", file_size);
        if (ftruncate(fd, file_size)) {
            perror("ftruncate");
            return 1;
        };
        sync_data(fd, nullptr, 0);

        char* base = nullptr;
        if (write_operation == WriteOperation::mmap) {
            base = (char*)mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (base == (char*)-1) {
                perror("mmap");
                return 1;
            }
        }

        char page_buffer[PAGE_SIZE];

        // Simulate updating the data portion of the file.
        fprintf(stderr, "Updating data portion of file...");
        for (size_t j = 1; j < page_count; ++j) {
            size_t offset = (page_count - j) * PAGE_SIZE;
            memset_pattern8(page_buffer, &j, PAGE_SIZE);
            if (write_data(fd, base, offset, page_buffer, PAGE_SIZE) != PAGE_SIZE) {
                perror("write_data");
                return 1;
            }
        }
        if (sync_data(fd, base, file_size)) {
            perror("sync_data");
            return 1;
        }
        fprintf(stderr, " done!\n");

        // Simulate updating the header portion of the file.
        fprintf(stderr, "Updating header portion of file...");
        memset_pattern8(page_buffer, &file_size, PAGE_SIZE);
        if (write_data(fd, base, 0, page_buffer, PAGE_SIZE) != PAGE_SIZE) {
            perror("write_data");
            return 1;
        }
        if (sync_data(fd, base, file_size)) {
            perror("sync_data");
            return 1;
        }
        fprintf(stderr, " done!\n");

        if (write_operation == WriteOperation::mmap) {
            if (munmap(base, file_size)) {
                perror("munmap");
                return 1;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return 0;
}
