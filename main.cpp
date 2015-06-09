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

void ensure(bool condition)
{
    if (!condition)
        throw std::system_error(errno, std::system_category());
}

class Writer;

class SyncOperation {
public:
    virtual void sync(const Writer&) = 0;
};

class Writer {
public:
    Writer(const std::string& directory, const std::string& file_name) : m_fd(-1), m_length(0)
    {
        std::string file_path = directory + "/" + file_name;
        m_fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
        ensure(m_fd != -1);

        std::unique_ptr<DIR, decltype(&closedir)> directory_handle { opendir(directory.c_str()), &closedir};
        ensure(directory_handle.get());

        int directory_fd = dirfd(directory_handle.get());
        ensure(directory_fd != -1);

        ensure(fsync(directory_fd) == 0);
    }

    virtual ~Writer()
    {
        close(m_fd);
    }

    int fileDescriptor() const
    {
        assert(m_fd != -1);
        return m_fd;
    }

    void setFileDescriptor(int fd)
    {
        m_fd = fd;
    }

    size_t length() const
    {
        return m_length;
    }

    virtual void *buffer() const
    {
        return nullptr;
    }

    void sync(const std::vector<SyncOperation*>& operations) const
    {
        for (auto* op : operations)
            op->sync(*this);
    }

    virtual void extend(off_t length)
    {
        ensure(ftruncate(m_fd, length) == 0);
        m_length = length;
    }

    virtual void write(off_t offset, void* data, size_t length) = 0;
protected:
    int m_fd;
    size_t m_length;
};

class PWriteWriter : public Writer {
public:
    static std::unique_ptr<Writer> create(const std::string& directory, const std::string& file_name)
    {
        return std::unique_ptr<Writer>(new PWriteWriter(directory, file_name));
    }

    using Writer::Writer;

    void write(off_t offset, void* data, size_t length)
    {
        ensure(pwrite(m_fd, data, length, offset) == length);
    }
};

class MMapWriter : public Writer {
public:
    static std::unique_ptr<Writer> create(const std::string& directory, const std::string& file_name)
    {
        return std::unique_ptr<Writer>(new MMapWriter(directory, file_name));
    }

    MMapWriter(const std::string& directory, const std::string& file_name)
        : Writer(directory, file_name)
        , m_buffer(nullptr)
    {}

    ~MMapWriter() { remap(m_length, 0); }

    void* buffer() const override
    {
        return m_buffer;
    }

    void extend(off_t length) override
    {
        off_t old_length = m_length;
        Writer::extend(length);

        remap(old_length, m_length);
    }

    void write(off_t offset, void* data, size_t length) override
    {
        assert(offset + length <= m_length);
        memcpy(static_cast<char*>(m_buffer) + offset, data, length);
    }

private:
    void remap(off_t old_length, off_t new_length)
    {
        if (m_buffer && old_length)
            ::munmap(m_buffer, old_length);
        if (!new_length)
            return;
        m_buffer = mmap(nullptr, new_length, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
        if (m_buffer == (void*)MAP_FAILED) {
            m_buffer = nullptr;
            throw std::system_error(errno, std::system_category());
        }
    }

    void *m_buffer;
};

class MSyncOperation : public SyncOperation {
public:
    void sync(const Writer& writer) override
    {
        void* buffer = writer.buffer();
        if (!buffer)
            return;

        size_t length = writer.length();
        if (!length)
            return;

        ensure(msync(buffer, length, MS_SYNC) == 0);
    }
};

class FSyncOperation : public SyncOperation {
public:
    void sync(const Writer& writer) override
    {
        ensure(fsync(writer.fileDescriptor()) == 0);
    }
};

class FullFSyncOperation : public SyncOperation {
public:
    void sync(const Writer& writer) override
    {
        ensure(fcntl(writer.fileDescriptor(), F_FULLFSYNC) == 0);
    }
};

std::vector<SyncOperation*> sync_operations;
std::function<std::unique_ptr<Writer> (std::string, std::string)> writer_factory;

std::string current_timestamp()
{
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d-%H-%M-%S", &tstruct);
    return buf;
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
            sync_operations.push_back(new MSyncOperation);
        else if (operation == "fsync")
            sync_operations.push_back(new FSyncOperation);
        else if (operation == "fullfsync")
            sync_operations.push_back(new FullFSyncOperation);
        else
            return 1;
    }

    std::string write_operation_string = argv[2];
    if (write_operation_string == "mmap")
        writer_factory = MMapWriter::create;
    else if (write_operation_string == "write")
        writer_factory = PWriteWriter::create;
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

    std::string test_file_name = "test-" + current_timestamp() + ".dat";
    fprintf(stderr, "Test file: %s\n", test_file_name.c_str());

    auto writer = writer_factory(working_directory, test_file_name);

    for (size_t i = 0; i < 1024; ++i) {
        size_t page_count = 10 + i * 10;
        size_t file_size = page_count * PAGE_SIZE;
        if (i > 0)
            fputc('\n', stderr);
        fprintf(stderr, "Truncating file to %zu bytes.\n", file_size);
        writer->extend(file_size);
        writer->sync(sync_operations);

        char page_buffer[PAGE_SIZE];

        // Simulate updating the data portion of the file.
        fprintf(stderr, "Updating data portion of file...");
        for (size_t j = 1; j < page_count; ++j) {
            size_t offset = (page_count - j) * PAGE_SIZE;
            memset_pattern8(page_buffer, &j, PAGE_SIZE);
            writer->write(offset, page_buffer, PAGE_SIZE);
        }
        writer->sync(sync_operations);
        fprintf(stderr, " done!\n");

        // Simulate updating the header portion of the file.
        fprintf(stderr, "Updating header portion of file...");
        memset_pattern8(page_buffer, &file_size, PAGE_SIZE);
        writer->write(0, page_buffer, PAGE_SIZE);
        writer->sync(sync_operations);
        fprintf(stderr, " done!\n");

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return 0;
}
