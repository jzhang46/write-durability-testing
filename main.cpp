#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

void ensure(bool condition)
{
    if (!condition)
        throw std::system_error(errno, std::system_category());
}

class WriteStrategy;

class SyncStrategy {
public:
    virtual void sync(const WriteStrategy&) = 0;
};

class WriteStrategy {
public:
    WriteStrategy(const std::string& directory, const std::string& file_name) : m_fd(-1), m_length(0)
    {
        std::string file_path = directory + "/" + file_name;
        m_fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
        ensure(m_fd != -1);

        DIR* directory_handle = opendir(directory.c_str());
        ensure(directory_handle);

        m_parentFD = dirfd(directory_handle);
        ensure(m_parentFD != -1);

        ensure(fsync(m_parentFD) == 0);
    }

    virtual ~WriteStrategy()
    {
        close(m_fd);
    }

    int fileDescriptor() const
    {
        assert(m_fd != -1);
        return m_fd;
    }

    int parentFileDescriptor() const
    {
        assert(m_parentFD != -1);
        return m_parentFD;
    }

    size_t length() const
    {
        return m_length;
    }

    virtual void *buffer() const
    {
        return nullptr;
    }

    void sync(const std::vector<SyncStrategy*>& strategies) const
    {
        for (auto* st : strategies)
            st->sync(*this);
    }

    virtual void extend(off_t length)
    {
        ensure(ftruncate(m_fd, length) == 0);
        m_length = length;
    }

    virtual void write(off_t offset, void* data, size_t length) = 0;
protected:
    int m_fd;
    int m_parentFD;
    size_t m_length;
};

class PWriteWriteStrategy : public WriteStrategy {
public:
    static std::unique_ptr<WriteStrategy> create(const std::string& directory, const std::string& file_name)
    {
        return std::unique_ptr<WriteStrategy>(new PWriteWriteStrategy(directory, file_name));
    }

    using WriteStrategy::WriteStrategy;

    void write(off_t offset, void* data, size_t length)
    {
        ensure(pwrite(m_fd, data, length, offset) == length);
    }
};

class MMapWriteStrategy : public WriteStrategy {
public:
    static std::unique_ptr<WriteStrategy> create(const std::string& directory, const std::string& file_name)
    {
        return std::unique_ptr<WriteStrategy>(new MMapWriteStrategy(directory, file_name));
    }

    MMapWriteStrategy(const std::string& directory, const std::string& file_name)
        : WriteStrategy(directory, file_name)
        , m_buffer(nullptr)
    {}

    ~MMapWriteStrategy() { remap(m_length, 0); }

    void* buffer() const override
    {
        return m_buffer;
    }

    void extend(off_t length) override
    {
        off_t old_length = m_length;
        WriteStrategy::extend(length);

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

class NoopSyncStrategy : public SyncStrategy {
public:
    void sync(const WriteStrategy& writer) override
    {
    }
};

class MSyncStrategy : public SyncStrategy {
public:
    void sync(const WriteStrategy& writer) override
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

class FSyncStrategy : public SyncStrategy {
public:
    void sync(const WriteStrategy& writer) override
    {
        ensure(fsync(writer.fileDescriptor()) == 0);
    }
};

class FSyncParentStrategy : public SyncStrategy {
public:
    void sync(const WriteStrategy& writer) override
    {
        ensure(fsync(writer.parentFileDescriptor()) == 0);
    }
};


class FullFSyncStrategy : public SyncStrategy {
public:
    void sync(const WriteStrategy& writer) override
    {
        ensure(fcntl(writer.fileDescriptor(), F_FULLFSYNC) == 0);
    }
};

std::vector<SyncStrategy*> write_sync_strategies;
std::vector<SyncStrategy*> extend_sync_strategies;
std::function<std::unique_ptr<WriteStrategy> (std::string, std::string)> writer_factory;

std::string current_timestamp()
{
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d-%H-%M-%S", &tstruct);
    return buf;
}

static const std::unordered_map<std::string, SyncStrategy*> sync_strategies_by_name = { {"none", new NoopSyncStrategy}, {"msync", new MSyncStrategy},
                                                                                        {"fsync", new FSyncStrategy}, {"fullfsync", new FullFSyncStrategy},
                                                                                        {"fsyncparent", new FSyncParentStrategy} };

std::vector<SyncStrategy*> sync_strategies_from_string(char* strategy_list_string)
{
    std::vector<SyncStrategy*> strategies;
    char* context;
    for (const char* strategy = strtok_r(strategy_list_string, ",", &context);
         strategy;
         strategy = strtok_r(nullptr, ",", &context)) {

        auto it = sync_strategies_by_name.find(strategy);
        if (it != sync_strategies_by_name.end())
            strategies.push_back(it->second);
        else
            throw std::domain_error("Unknown sync strategy");
    }
    return strategies;
}

void initialize_from_arguments(int argc, char** argv)
{
    if (argc != 4)
        throw std::length_error("Expected 4 arguments.");

    std::string write_strategy_string = argv[1];
    if (write_strategy_string == "mmap")
        writer_factory = MMapWriteStrategy::create;
    else if (write_strategy_string == "write")
        writer_factory = PWriteWriteStrategy::create;
    else
        throw std::domain_error("Unknown write strategy");

    write_sync_strategies = sync_strategies_from_string(argv[2]);
    extend_sync_strategies = sync_strategies_from_string(argv[3]);
}

int main(int argc, char** argv)
{
    try {
        initialize_from_arguments(argc, argv);
    } catch (const std::exception& e) {
        fprintf(stderr, "%s\n", e.what());
        fprintf(stderr, "Usage: main [mmap|write] write-sync-strategy-list extend-sync-strategy-list\n");
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

    // Simulate a series of transactional writes to the file.
    // The file size is increased by 16 pages after every 128 writes.
    // The 128 writes correspond to updating each of the 16 new pages 8 times.
    // Each write consists of writing a full page of data, followed by updating
    // the index on page 0 to reflect the newly-written data.
    const size_t file_page_count_increment = 16;
    const size_t versions_per_file_size = 8;

    for (size_t i = 0; i < 1024; ++i) {
        size_t page_count = file_page_count_increment * (i + 1) + 1;
        size_t file_size = page_count * PAGE_SIZE;
        if (i > 0)
            fputc('\n', stderr);

        fprintf(stderr, "Truncating file to %zu bytes.\n", file_size);
        writer->extend(file_size);
        writer->sync(extend_sync_strategies);

        size_t base_offset = (page_count - file_page_count_increment) * PAGE_SIZE;
        for (size_t j = 0; j < file_page_count_increment * versions_per_file_size; ++j) {
            char page_buffer[PAGE_SIZE];

            // Simulate updating the data portion of the file.
            size_t index = j % file_page_count_increment;
            size_t version = j / file_page_count_increment;
            size_t offset = base_offset + index * PAGE_SIZE;
            fprintf(stderr, "Writing index %zu, version %zu at offset %zu...", index, version, offset);
            struct { size_t a, b; } pattern = { index, version };
            memset_pattern16(page_buffer, &pattern, PAGE_SIZE);
            writer->write(offset, page_buffer, PAGE_SIZE);

            writer->sync(write_sync_strategies);
            fprintf(stderr, " done!\n");

            // Simulate updating the header portion of the file.
            fprintf(stderr, "Updating header portion of file...");
            struct { size_t a, b, c, d; } header = { base_offset, index, version, std::numeric_limits<size_t>::max() };
            writer->write(index * sizeof(header), &header, sizeof(header));
            writer->sync(write_sync_strategies);
            fprintf(stderr, " done!\n");

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    return 0;
}
