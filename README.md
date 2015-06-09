# OS X file durability testing

This is a simple test application to verify the durability provided by OS X when updating a file on disk shortly prior to the computer being unexpectedly restarted.
It can test writing via `mmap` or `pwrite` with various combinations of `msync`, `fsync` and the `F_FULLFSYNC` `fcntl` for synchronization.
Note that some combinations are not valid: `msync` requires a memory mapped buffer that is not available when using `pwrite`.

## Building and running

1. `make`
2. `rm -f working/ && ./main mmap msync`
3. Kill power to the machine.
4. `./verify working/test-*`

## Observed results

Testing was performed on a Mac mini with an SSD running OS X 10.10.2, plugged into a power brick with an on-off switch.
The program was launched, and after around 1MB of data had been written the power was turned off.
After restoring power the `verify` command was used to examine the state of the file on disk.

### mmap

| sync strategy         | result |
|-----------------------|----------------------------------------------------------------------------|
| None                  | 0 byte file |
| `msync`               | header points past end of file (file size not synced before header synced) |
| `msync` + `fsync`     | header points past end of file (file size not synced before header synced) |
| `msync` + `fullfsync` | success! |
| `fsync`               | 0 byte file |
| `fullfsync`           | correct file size, file data all zeroes |


### write

| sync strategy         | result |
|-----------------------|----------------------------------------------------------------------------|
| None                  | file size consistent with much older transaction (last system sync? luck?) |
| `fsync`               | header points past end of file (file size not synced before header synced) |
| `fullfsync`           | success! |

## Future improvements

* Allow for transactions that don't extend the file size.
* Allow for diffferent sync strategies when extending the file size vs writing data.
